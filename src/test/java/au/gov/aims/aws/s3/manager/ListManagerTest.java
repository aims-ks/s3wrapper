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
import au.gov.aims.aws.s3.entity.S3Client;
import au.gov.aims.aws.s3.entity.S3File;
import au.gov.aims.aws.s3.entity.S3List;
import com.amazonaws.services.s3.AmazonS3URI;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ListManagerTest extends S3TestBase {
    private static final Logger LOGGER = Logger.getLogger(ListManagerTest.class);

    /**
     * Upload a file to S3, then download it to see if it has changed.
     * NOTE: The resource file "aws-credentials.properties" must be set before running this test.
     * @throws Exception If something goes wrong...
     */
    @Test
    public void testLs() throws Exception {
        try (S3Client client = super.openS3Client()) {
            super.setupBucket(client);

            AmazonS3URI s3Uri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "/bin/");
            S3List s3List = ListManager.ls(client, s3Uri);
            LOGGER.info(s3List);

            Assert.assertNotNull("The file list is null", s3List);

            Map<String, S3File> dirs = s3List.getDirs();
            Assert.assertTrue("Found sub-directory in 'bin'.", dirs == null || dirs.isEmpty());

            Map<String, S3File> files = s3List.getFiles();
            Assert.assertEquals("Wrong number of files in 'bin'.", 3, files.size());

            Assert.assertTrue("Missing file 'bin/random_100.bin'.", files.containsKey("bin/random_100.bin"));
            Assert.assertTrue("Missing file 'bin/random_1024.bin'.", files.containsKey("bin/random_1024.bin"));
            Assert.assertTrue("Missing file 'bin/zero_100.bin'.", files.containsKey("bin/zero_100.bin"));
        }
    }
}
