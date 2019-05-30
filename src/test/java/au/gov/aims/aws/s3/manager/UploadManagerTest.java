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

import au.gov.aims.aws.s3.Md5;
import au.gov.aims.aws.s3.S3TestBase;
import au.gov.aims.aws.s3.S3Utils;
import au.gov.aims.aws.s3.entity.S3Client;
import au.gov.aims.aws.s3.entity.S3File;
import au.gov.aims.aws.s3.entity.S3List;
import com.amazonaws.services.s3.AmazonS3URI;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;

public class UploadManagerTest extends S3TestBase {
    private static final Logger LOGGER = Logger.getLogger(UploadManagerTest.class);

    /**
     * Upload a file to S3, then download it to see if it has changed.
     * NOTE: The resource file "aws-credentials.properties" must be set before running this test.
     * @throws Exception If something goes wrong...
     */
    @Test
    public void testUploadDownloadFile() throws Exception {
        URL origFileUrl = UploadManagerTest.class.getClassLoader().getResource("bucket_files/bin/random_1024.bin");
        File origFile = new File(origFileUrl.toURI());
        File tempFile = File. createTempFile("s3mockup_", "_random_1024.bin");
        AmazonS3URI destinationUri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "/bin/random_1024.bin");

        try (S3Client client = super.openS3Client()) {
            super.setupBucket(client);

            S3List uploadS3List = UploadManager.upload(client, origFile, destinationUri);
            LOGGER.info(uploadS3List);

            // Verify upload
            Assert.assertNotNull("The upload response is null.", uploadS3List);

            // Verify if the file is on S3
            S3List checkS3List = ListManager.ls(client, destinationUri);
            LOGGER.info(checkS3List);

            Assert.assertNotNull("The file info response is null.", checkS3List);

            // Get the file from the map
            S3File checkS3File = checkS3List.getFiles().get("bin/random_1024.bin");
            Assert.assertNotNull("The uploaded file is null", checkS3File);
            Assert.assertEquals("The uploaded file key is wrong", "bin/random_1024.bin", checkS3File.getS3Uri().getKey());
            Assert.assertEquals("The uploaded file size do not match the original", origFile.length(), checkS3File.getFileSize().longValue());


            // Download the file
            S3List downloadS3List = DownloadManager.download(client, destinationUri, tempFile);
            LOGGER.info(downloadS3List);

            // Verify download
            Assert.assertNotNull("The download response is null.", downloadS3List);

            // Check file md5sum
            String md5sumOrig = Md5.md5sum(origFile);
            String md5sumDownloaded = Md5.md5sum(tempFile);
            Assert.assertEquals("The file md5sum doesn't match.", md5sumOrig, md5sumDownloaded);

            // Check last modified
            long lastModifiedCheck = checkS3File.getLastModified();
            long lastModifiedDownloaded = tempFile.lastModified();
            Assert.assertEquals("The file last modified data doesn't match.", lastModifiedCheck, lastModifiedDownloaded);
        }
    }
}
