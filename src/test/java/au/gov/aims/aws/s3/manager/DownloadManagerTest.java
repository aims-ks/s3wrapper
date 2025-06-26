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
import au.gov.aims.aws.s3.entity.S3ClientWrapper;
import au.gov.aims.aws.s3.entity.S3File;
import au.gov.aims.aws.s3.entity.S3List;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Uri;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class DownloadManagerTest extends S3TestBase {
    private static final Logger LOGGER = Logger.getLogger(DownloadManagerTest.class);

    /**
     * Upload a file to S3, then download it to see if it has changed.
     * NOTE: The resource file "aws-credentials.properties" must be set before running this test.
     * @throws Exception If something goes wrong...
     */
    @Test(expected = FileNotFoundException.class)
    public void testDownloadFileNotFound() throws Exception {
        try (S3ClientWrapper client = super.openS3Client()) {
            super.setupBucket(client);

            S3Uri s3Uri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "/none-existing-folder/nope/this-file-does-not-exists.png");
            DownloadManager.download(client, s3Uri, new File("/tmp/s3wrapper"));

            Assert.fail("Downloading a non existing file must trigger an FileNotFoundException.");
        }
    }

    /**
     * Upload a file to S3, then download it to see if it has changed.
     * NOTE: The resource file "aws-credentials.properties" must be set before running this test.
     * @throws Exception If something goes wrong...
     */
    @Test
    public void testDownloadFile() throws Exception {
        URL origFileUrl = DownloadManagerTest.class.getClassLoader().getResource("bucket_files/bin/random_100.bin");
        File origFile = new File(origFileUrl.toURI());
        File destinationFile = new File("/tmp/s3wrapper/random_100.bin");

        try (S3ClientWrapper client = super.openS3Client()) {
            super.setupBucket(client);

            S3Uri s3Uri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "/bin/random_100.bin");
            S3List s3List = DownloadManager.download(client, s3Uri, destinationFile);
            LOGGER.info(s3List);

            Assert.assertNotNull("The downloaded file list is null", s3List);

            Map<String, S3File> dirs = s3List.getDirs();
            Assert.assertTrue("Some directories were downloaded.", dirs == null || dirs.isEmpty());

            Map<String, S3File> files = s3List.getFiles();
            Assert.assertEquals("Wrong number of downloaded files.", 1, files.size());

            // Get the file from the map
            S3File file = files.get("bin/random_100.bin");
            Assert.assertEquals("Missing file 'bin/random_100.bin'.", "bin/random_100.bin",
                    file.getS3Uri().key().orElse(null));
            Assert.assertTrue("The destination file was not created.", destinationFile.exists());

            // Check file size
            Long origFileSize = origFile.length();
            Long destinationFileSize = destinationFile.length();
            Long metadataFileSize = file.getFileSize(client);

            Assert.assertEquals("The file size in the file metadata differ from the original file size.", metadataFileSize, origFileSize);
            Assert.assertEquals("The downloaded file size differ from the original file size.", destinationFileSize, origFileSize);

            // Check file md5sum
            String md5sumOrig = Md5.md5sum(origFile);
            String md5sumDownloaded = Md5.md5sum(destinationFile);
            Assert.assertEquals("The downloaded file md5sum doesn't match original file.", md5sumOrig, md5sumDownloaded);

            // Check file lastModified date
            Long downloadedLastModified = file.getLastModified(client);
            Assert.assertNotNull("The downloaded file lastModified metadata attribute is null.", downloadedLastModified);
            Assert.assertEquals("The downloaded file lastModified metadata attribute do not actual file lastModified attribute.",
                    downloadedLastModified.longValue(), destinationFile.lastModified());
        }
    }

    /**
     * Upload a file to S3, then download it to see if it has changed.
     * NOTE: The resource file "aws-credentials.properties" must be set before running this test.
     * @throws Exception If something goes wrong...
     */
    @Test
    public void testDownloadFilesPattern() throws Exception {
        File destinationFolder = new File("/tmp/s3wrapper/random_star");

        try (S3ClientWrapper client = super.openS3Client()) {
            super.setupBucket(client);

            S3Uri s3Uri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "/bin/random_*.bin");
            S3List s3List = DownloadManager.download(client, s3Uri, destinationFolder);
            LOGGER.info(s3List);

            Assert.assertNotNull("The downloaded file list is null", s3List);

            // Check the output folder
            Assert.assertTrue("The output directory was not created", destinationFolder.exists());


            File[] downloadedFiles = destinationFolder.listFiles();
            Assert.assertNotNull("The output directory is empty", downloadedFiles);
            Assert.assertEquals("The output directory should contain 2 files", 2, downloadedFiles.length);

            // Create a map with the downloaded files, to make it easier to look for them.
            Map<String, File> downloadedFileMap = new HashMap<String, File>();
            for (File downloadedFile : downloadedFiles) {
                downloadedFileMap.put(downloadedFile.getName(), downloadedFile);
            }

            Assert.assertTrue("The file 'random_100.bin' has not been downloaded", downloadedFileMap.containsKey("random_100.bin"));
            Assert.assertTrue("The file 'random_1024.bin' has not been downloaded", downloadedFileMap.containsKey("random_1024.bin"));
        }
    }

}
