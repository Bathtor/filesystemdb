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

import java.io.File;

/**
 *
 * @author lkroll
 */
public class Configuration {
    
    final File path;
    int cacheSize = 10;
    
    public Configuration(File path) {
        this.path = path;
    }

    /**
     * @return the path
     */
    public File getPath() {
        return path;
    }

    /**
     * @return the cacheSize
     */
    public int getCacheSize() {
        return cacheSize;
    }

    /**
     * @param cacheSize the cacheSize to set
     */
    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }
}
