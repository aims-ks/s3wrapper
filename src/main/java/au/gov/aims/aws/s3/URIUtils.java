/*
 *  Copyright (C) 2019 Australian Institute of Marine Science
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

import java.net.URI;
import java.net.URISyntaxException;

public class URIUtils {

    public static URI getParentUri(URI uri) throws URISyntaxException {
        if (uri == null) {
            return null;
        }

        String path = uri.getPath();
        if (path == null || path.isEmpty()) {
            return uri;
        }

        if (path.endsWith("/")) {
            // Remove the trailing slash
            path = path.substring(0, path.length() - 2);
        }

        int lastSlashIdx = path.lastIndexOf('/');

        String parentPath;
        if (lastSlashIdx < 0) {
            parentPath = "";
        } else {
            parentPath = path.substring(0, lastSlashIdx + 1);
        }

        return new URI(uri.getScheme(), uri.getHost(), parentPath, null);
    }

    public static String getFilename(URI uri) {
        if (uri == null) {
            return null;
        }

        String path = uri.getPath();
        if (path == null || path.isEmpty() || path.endsWith("/")) {
            return null;
        }

        int lastSlashIdx = path.lastIndexOf('/');

        return lastSlashIdx < 0 ? path : path.substring(lastSlashIdx + 1);
    }
}
