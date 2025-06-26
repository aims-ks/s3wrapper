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
package au.gov.aims.aws.s3.manager;

import au.gov.aims.aws.s3.S3TestBase;
import au.gov.aims.aws.s3.S3Utils;
import au.gov.aims.aws.s3.entity.S3ClientWrapper;
import au.gov.aims.aws.s3.entity.S3File;
import au.gov.aims.aws.s3.entity.S3List;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.util.Map;

public class ListManagerTestManual extends S3TestBase {

    /**
     * There was a missing S3Object.close() in ListManager which was causing the AWS library to timeout.
     * I created this manual test to replicate the bug and verify if it was fixed.
     */
    @Test
    @Ignore
    public void testListingLargeRepository() throws Exception {
        String s3UriStr = "s3://aims-ereefs-public-test/ncanimate/products/products__ncanimate__ereefs__gbr4_v2__temp-wind-salt-current_hourly/";

        try (S3ClientWrapper client = super.openS3Client()) {
            S3List list = ListManager.ls(client, S3Utils.getS3URIFromURI(new URI(s3UriStr)), false);

            for (Map.Entry<String, S3File> dirEntry : list.getDirs().entrySet()) {
                System.out.println("DIR: " + dirEntry.getKey());
            }
            for (Map.Entry<String, S3File> fileEntry : list.getFiles().entrySet()) {
                System.out.println("FILE: " + fileEntry.getKey());
            }
        }
    }
}
