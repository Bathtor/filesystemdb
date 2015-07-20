/*
 * Copyright (C) 2015 lkroll
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package com.larskroll.fsdb;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.larskroll.common.ByteArrayFormatter;
import com.larskroll.common.DataRef;
import com.larskroll.common.LRUCache;
import com.larskroll.common.LRUCache.EvictionHandler;
import com.larskroll.common.RAFileRef;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author lkroll
 */
public class FileSystemDB implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemDB.class);
    private static final Comparator<byte[]> COMP = UnsignedBytes.lexicographicalComparator();

    private final Configuration config;
    private final File dbFolder;
    private final LRUCache<File, RAFileRef> cache;
    private final LRUCache<Byte, File> subdirs;

    public FileSystemDB(Configuration config) throws FSDBException {
        this.config = config;
        this.dbFolder = config.getPath();
        if (!dbFolder.canWrite() || !dbFolder.canRead()) {
            throw new FSDBException("FSDB doesn't have sufficient rights to write in " + dbFolder.getAbsolutePath());
        }
        LOG.info("Started on {}", dbFolder);
        cache = new LRUCache<File, RAFileRef>(config.cacheSize, new EvictionHandler<File, RAFileRef>() {

            public void evicted(Map.Entry<File, RAFileRef> entry) {
                entry.getValue().release();
            }
        });
        subdirs = new LRUCache<Byte, File>(Byte.MAX_VALUE * 2);
    }

    public void put(byte[] key, DataRef value, int version) {
        RAFileRef fr = null;
        try {
            File f = key2File(key, version);
            ensureExists(f, key);
            //LOG.trace("Starting PUT into {}", f);
            fr = ref4File(f);
            RandomAccessFile raf = fr.getRAF();
            raf.setLength(value.size());
            raf.seek(0);
            if (value.size() <= 0) {
                return; // no reason to write nothing^^
            }
            if (value.size() < Integer.MAX_VALUE) { // can buffer through byte[]
                raf.write(value.dereference());
            } else if (value instanceof RAFileRef) { // too big for byte array but can do OS level file2file transfer
                RAFileRef src = (RAFileRef) value;
                FileChannel sink = raf.getChannel();
                FileChannel source = src.getRAF().getChannel();
                source.transferTo(0, src.size(), sink);
            } else { // too big for single byte array must buffer partial ranges...this not very efficient
                byte[] buffer;
                long num = value.size() / Integer.MAX_VALUE;
                int rest = (int) (value.size() - num * Integer.MAX_VALUE);
                for (long i = 0; i < num; i++) {
                    long start = i * Integer.MAX_VALUE;
                    long end = start + Integer.MAX_VALUE;
                    raf.write(value.dereference(start, end));
                }
                long start = num * Integer.MAX_VALUE;
                long end = start + rest;
                raf.write(value.dereference(start, end));
            }
        } catch (IOException ex) {
            LOG.error("Could not perform PUT operation: ", ex);
        } finally {
            if (fr != null) {
                fr.release();
            }
        }
    }

    public SortedMap<Integer, RAFileRef> get(byte[] key) {
        SortedMap<Integer, RAFileRef> data = new TreeMap<Integer, RAFileRef>();
        try {
            SortedMap<Integer, File> versions = key2Files(key);

            get(versions, data);
        } catch (IOException ex) {
            LOG.error("Could not perform GET operation: ", ex);
        }
        return data;
    }

    public RAFileRef get(byte[] key, int version) {
        try {
            SortedMap<Integer, File> versions = key2Files(key);
            //LOG.trace("Found {} version for key {}", versions.size(), key);
            return get(versions, version);
        } catch (IOException ex) {
            LOG.error("Could not perform GET operation: ", ex);
        }
        return null;
    }

    public Pair<Integer, RAFileRef> getCurrent(byte[] key) {
        try {
            SortedMap<Integer, File> versions = key2Files(key);
            return getCurrent(versions);
        } catch (IOException ex) {
            LOG.error("Could not perform GET operation: ", ex);
        }
        return Pair.with(0, null);
    }

    public void delete(byte[] key, int version) {
        RAFileRef fr = get(key, version);
        if (fr == null) {
            return;
        }
        try {
            fr.getRAF().setLength(0);
            fr.markForDeletion();
            if (cache.remove(fr.getFile()) != null) {
                fr.release(); // balance the cache retain
            }
            fr.release(); // balance out the original value
        } catch (IOException ex) {
            LOG.error("Could not perform DELETE operation: ", ex);
        }
    }

    public void delete(byte[] key) {
        SortedMap<Integer, RAFileRef> refs = get(key);
        if (refs.isEmpty()) {
            return;
        }
        try {
            for (RAFileRef fr : refs.values()) {
                fr.getRAF().setLength(0);
                fr.markForDeletion();
                if (cache.remove(fr.getFile()) != null) {
                    fr.release(); // balance the cache retain
                }
                fr.release(); // balance out the original value
            }
        } catch (IOException ex) {
            LOG.error("Could not perform DELETE operation: ", ex);
        }
    }

    public Iterator<KeyPointer> iterator() {
        File[] dirs = dbFolder.listFiles(new FileFilter() {

            public boolean accept(File pathname) {
                return pathname.isDirectory() && (pathname.getName().length() == 4);
            }
        });
        LinkedListMultimap<ByteBuffer, File> files = LinkedListMultimap.create();
        for (File dir : dirs) {
            File[] vals = dir.listFiles(new FilenameFilter() {

                public boolean accept(File dir, String name) {
                    return name.endsWith(".val");
                }
            });
            for (File val : vals) {
                String fname = val.getName();
                String[] part = fname.split("v");
                byte[] key = ByteArrayFormatter.parseStoreFormat(part[0]);
                files.put(ByteBuffer.wrap(key), val);
            }
        }
        ArrayList<KeyPointer> keys = new ArrayList<KeyPointer>(files.keySet().size());
        for (ByteBuffer b : files.keySet()) {
            SortedMap<Integer, File> versions = new TreeMap<Integer, File>();
            for (File f : files.get(b)) {
                String fname = f.getName();
                String name = fname.substring(0, fname.length() - 4); // drop suffix
                String[] parts = name.split("v");
                int version = Integer.parseInt(parts[1]);
                versions.put(version, f);
            }
            KeyPointer kp = new KeyPointer(b.array(), versions);
            keys.add(kp);
        }
        return keys.iterator();
    }

    public void close() {
        for (RAFileRef fr : cache.values()) {
            fr.release();
        }
        cache.clear();
        subdirs.clear();
        LOG.info("Closed database at {}", this.dbFolder);
    }

    private void get(SortedMap<Integer, File> versions, SortedMap<Integer, RAFileRef> data) throws FileNotFoundException {
        for (Entry<Integer, File> e : versions.entrySet()) {
            RAFileRef fr = ref4File(e.getValue());
            data.put(e.getKey(), fr);
        }
    }

    private RAFileRef get(SortedMap<Integer, File> versions, int version) throws FileNotFoundException {
        File f = versions.get(version);
        if (f != null) {
            RAFileRef fr = ref4File(f);
            return fr;
        } else {
            //LOG.trace("No version {}", version);
        }
        return null;
    }

    private Pair<Integer, RAFileRef> getCurrent(SortedMap<Integer, File> versions) throws FileNotFoundException {
        if (!versions.isEmpty()) {
            File f = versions.get(versions.lastKey());
            RAFileRef fr = ref4File(f);
            return Pair.with(versions.lastKey(), fr);
        }
        return Pair.with(0, null);
    }

    private RAFileRef ref4File(File f) throws FileNotFoundException {
        RAFileRef fr = cache.get(f);
        if (fr == null) {
            RandomAccessFile raf = new RandomAccessFile(f, "rws");
            fr = new RAFileRef(f, raf);
            fr.retain(); // this one is for the cache
            cache.put(f, fr);
        } else {
            fr.retain(); // this one is for the return value
        }
        return fr;
    }

    private File key2File(byte[] key, int version) throws IOException {
        String keyStr = ByteArrayFormatter.storeFormat(key);
        StringBuilder sb = new StringBuilder();
        sb.append(dbFolder.getCanonicalPath());
        sb.append(File.separatorChar);
        sb.append(keyStr.substring(0, 4));
        sb.append(File.separatorChar);
        sb.append(keyStr);
        sb.append('v');
        sb.append(version);
        sb.append(".val");
        return new File(sb.toString());
    }

    private SortedMap<Integer, File> key2Files(byte[] key) throws IOException {
        final String keyStr = ByteArrayFormatter.storeFormat(key);
        Byte prefix = key[0];
        File dir = subdirs.get(prefix);
        if (dir == null) {
            String dirStr = ByteArrayFormatter.storeFormat(new byte[]{key[0]});
            dir = new File(dbFolder.getCanonicalPath() + File.separatorChar + dirStr);
        }
        SortedMap<Integer, File> versions = new TreeMap<Integer, File>();
        if (!dir.exists() || !dir.isDirectory()) {
            return versions;
        }
        File[] files = dir.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return name.startsWith(keyStr) && name.endsWith(".val");
            }
        });
        for (File f : files) {
            String fname = f.getName();
            String name = fname.substring(0, fname.length() - 4); // drop suffix
            String[] parts = name.split("v");
            int version = Integer.parseInt(parts[1]);
            versions.put(version, f);
        }
        return versions;
    }

    private void ensureExists(File f, byte[] key) throws IOException {
        if (f.exists()) {
            return;
        }
        Byte prefix = key[0];
        File dirF = subdirs.get(prefix);
        if (dirF == null) {
            String dirStr = ByteArrayFormatter.storeFormat(new byte[]{key[0]});
            dirF = new File(dbFolder.getCanonicalPath() + File.separatorChar + dirStr);
        }
        if (!dirF.exists()) {
            dirF.mkdir();
        }
        f.createNewFile();
    }

    public class KeyPointer implements Comparable<KeyPointer> {

        private final byte[] key;
        private final SortedMap<Integer, File> versions;

        KeyPointer(byte[] key, SortedMap<Integer, File> versions) {
            this.key = key;
            this.versions = versions;
        }

        public byte[] getKey() {
            return this.key;
        }

        public Set<Integer> getVersions() {
            return (SortedSet<Integer>) versions.keySet();
        }

        public SortedMap<Integer, RAFileRef> getValues() {
            SortedMap<Integer, RAFileRef> data = new TreeMap<Integer, RAFileRef>();
            try {
                get(versions, data);
            } catch (FileNotFoundException ex) {
                LOG.error("Could not perform GET operation: ", ex);
            }
            return data;
        }

        public RAFileRef getValue(int version) {
            try {
                return get(versions, version);
            } catch (FileNotFoundException ex) {
                LOG.error("Could not perform GET operation: ", ex);
            }
            return null;
        }

        public RAFileRef getCurrentValue() {
            try {
                return getCurrent(versions).getValue1();
            } catch (FileNotFoundException ex) {
                LOG.error("Could not perform GET operation: ", ex);
            }
            return null;
        }

        public int compareTo(KeyPointer that) {
            return COMP.compare(this.key, that.key);
        }
    }

}
