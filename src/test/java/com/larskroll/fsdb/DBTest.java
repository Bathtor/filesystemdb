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

import com.larskroll.common.ByteArrayFormatter;
import com.larskroll.common.ByteArrayRef;
import com.larskroll.fsdb.FileSystemDB.FileRef;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import junit.framework.Assert;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 * @author lkroll
 */
@RunWith(JUnit4.class)
public class DBTest {

    private static final int NUM = 50;
    private static final int KEY_LENGTH = 20;
    private static final Random RAND = new Random(0);

    private final List<ByteBuffer> keys = new ArrayList<ByteBuffer>(NUM);
    private FileSystemDB fsdb;

    @Before
    public void setUp() {
        for (int i = 0; i < NUM; i++) {
            byte[] k = new byte[NUM];
            RAND.nextBytes(k);
            keys.add(ByteBuffer.wrap(k));
        }
        try {
            File dbP = Files.createTempDirectory("fsdbtest").toFile();
            dbP.deleteOnExit();

            Configuration fsconf = new Configuration(dbP);
            fsdb = new FileSystemDB(fsconf);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void dbTest() {
        // PUT
        for (int i = 0; i < NUM; i++) {
            ByteArrayRef bar = ByteArrayRef.wrap(keys.get(i).array());
            fsdb.put(keys.get(i).array(), bar, 0);
        }

        // GET
        for (int i = 0; i < NUM; i++) {
            FileRef val = fsdb.get(keys.get(i).array(), 0);
            assertNotNull(val);
            ByteBuffer valV = ByteBuffer.wrap(val.dereference());
            System.out.println("expected:\n     " + ByteArrayFormatter.printFormat(keys.get(i).array()));
            System.out.println("got:\n     " + ByteArrayFormatter.printFormat(valV.array()));
            assertEquals(keys.get(i), valV);
            val.release();
        }

        // DELETE
        for (int i = 0; i < NUM; i++) {
            fsdb.delete(keys.get(i).array(), 0); // this is eventual, so can't really test it
        }
    }

    @After
    public void tearDown() {
        keys.clear();
        fsdb.close();
        fsdb = null;
    }

}
