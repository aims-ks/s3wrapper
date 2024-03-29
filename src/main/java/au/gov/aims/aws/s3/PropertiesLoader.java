/*
 *  Copyright (C) 2018 Australian Institute of Marine Science
 *
 *  Contact: Gael Lafond <g.lafond@aims.gov.au>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package au.gov.aims.aws.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {

    public static Properties load(File file) throws IOException {
        Properties properties = null;
        InputStream input = null;

        try {
            input = new FileInputStream(file);
            properties = PropertiesLoader.load(input);
        } finally {
            if (input != null) {
                input.close();
            }
        }

        return properties;
    }

    public static Properties load(String classpath) throws IOException {
        Properties properties = null;
        InputStream input = null;

        try {
            input = PropertiesLoader.class.getClassLoader().getResourceAsStream(classpath);
            if (input != null) {
                properties = PropertiesLoader.load(input);
            }
        } finally {
            if (input != null) {
                input.close();
            }
        }

        return properties;
    }

    public static Properties load(InputStream inputStream) throws IOException {
        Properties properties = new Properties();

        properties.load(inputStream);

        return properties;
    }
}
