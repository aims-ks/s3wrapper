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

import au.gov.aims.aws.s3.entity.S3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;

public class FileWrapperTest extends S3TestBase {
    private static final Logger LOGGER = Logger.getLogger(FileWrapperTest.class);

    @Test
    public void testGetParent() throws Exception {
        AmazonS3URI s3Uri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "s3wrapper/FileWrapper/file.txt");
        File ioFile = new File("/tmp/s3wrapper/FileWrapper/file.txt");
        FileWrapper fileWrapper = new FileWrapper(s3Uri, ioFile);

        Assert.assertNotNull("The file wrapper is null", fileWrapper);

        FileWrapper parentFileWrapper = fileWrapper.getParent();

        Assert.assertNotNull("The parent file wrapper is null", parentFileWrapper);
        Assert.assertEquals("The parent file is wrong", new File("/tmp/s3wrapper/FileWrapper"), parentFileWrapper.getFile());
        Assert.assertEquals("The parent S3URI is wrong", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "s3wrapper/FileWrapper/"), parentFileWrapper.getS3URI());


        FileWrapper grandParentFileWrapper = parentFileWrapper.getParent();
        Assert.assertNotNull("The grand parent file wrapper is null", grandParentFileWrapper);
        Assert.assertEquals("The grand parent file is wrong", new File("/tmp/s3wrapper"), grandParentFileWrapper.getFile());
        Assert.assertEquals("The grand parent S3URI is wrong", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "s3wrapper/"), grandParentFileWrapper.getS3URI());


        FileWrapper grandGrandParentFileWrapper = grandParentFileWrapper.getParent();
        Assert.assertNotNull("The grand grand parent file wrapper is null", grandGrandParentFileWrapper);
        Assert.assertEquals("The grand grand parent file is wrong", new File("/tmp"), grandGrandParentFileWrapper.getFile());
        Assert.assertEquals("The grand grand parent S3URI is wrong", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "/"), grandGrandParentFileWrapper.getS3URI());


        s3Uri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "file.txt");
        ioFile = new File("/file.txt");
        fileWrapper = new FileWrapper(s3Uri, ioFile);

        parentFileWrapper = fileWrapper.getParent();
        FileWrapper childFileWrapper = new FileWrapper(parentFileWrapper, "file.json");
        Assert.assertNotNull("The child file wrapper is null", childFileWrapper);
        Assert.assertEquals("The child file wrapper file is wrong", new File("/file.json"), childFileWrapper.getFile());
        Assert.assertEquals("The child file wrapper S3URI is wrong", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "file.json"), childFileWrapper.getS3URI());
    }

    @Test
    public void testListFilesWithoutS3() throws Exception {
        URL ioFileUrl = FileWrapperTest.class.getClassLoader().getResource("bucket_files");
        File ioFile = new File(ioFileUrl.toURI());
        FileWrapper rootFileWrapper = new FileWrapper((URI)null, ioFile);

        List<FileWrapper> fileWrapperList = rootFileWrapper.listFiles(null);
        Assert.assertNotNull("The file list is null", fileWrapperList);
        Assert.assertEquals("Wrong number of files in the bucket.", 3, fileWrapperList.size());

        for (FileWrapper fileWrapper : fileWrapperList) {
            String filename = fileWrapper.getFile().getName();

            if ("bin".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "bin"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

                // List files in "bin"
                List<FileWrapper> binFileWrapperList = fileWrapper.listFiles(null);
                Assert.assertNotNull("The 'bin' file list is null", binFileWrapperList);
                Assert.assertEquals("Wrong number of files in the 'bin'.", 3, binFileWrapperList.size());

                // Check files in "bin"
                for (FileWrapper binFileWrapper : binFileWrapperList) {
                    String binFilename = binFileWrapper.getFile().getName();

                    if ("random_100.bin".equals(binFilename)) {
                        Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/random_100.bin"), binFileWrapper.getFile());
                        Assert.assertNull("The file wrapper is associated with a s3URI.", binFileWrapper.getS3URI());

                    } else if ("random_1024.bin".equals(binFilename)) {
                        Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/random_1024.bin"), binFileWrapper.getFile());
                        Assert.assertNull("The file wrapper is associated with a s3URI.", binFileWrapper.getS3URI());

                    } else if ("zero_100.bin".equals(binFilename)) {
                        Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/zero_100.bin"), binFileWrapper.getFile());
                        Assert.assertNull("The file wrapper is associated with a s3URI.", binFileWrapper.getS3URI());
                        Assert.assertTrue(String.format("The bin file %s does not exist.", binFileWrapper.getFile()), binFileWrapper.getFile().exists());

                    } else {
                        Assert.fail(String.format("Unexpected file key in 'bin': '%s'", binFilename));
                    }
                }

            } else if ("img".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "img"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("root.txt".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "root.txt"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());
                Assert.assertTrue(String.format("The file %s does not exist.", fileWrapper.getFile()), fileWrapper.getFile().exists());


            } else {
                Assert.fail(String.format("Unexpected filename: '%s'", filename));
            }
        }
    }

    @Test
    public void testListFilesRecursivelyWithoutS3() throws Exception {
        URL ioFileUrl = FileWrapperTest.class.getClassLoader().getResource("bucket_files");
        File ioFile = new File(ioFileUrl.toURI());
        FileWrapper rootFileWrapper = new FileWrapper((URI)null, ioFile);

        List<FileWrapper> fileWrapperList = rootFileWrapper.listFiles(null, true);
        Assert.assertNotNull("The file list is null", fileWrapperList);
        Assert.assertEquals("Wrong number of files in the bucket.", 9, fileWrapperList.size());

        for (FileWrapper fileWrapper : fileWrapperList) {
            String filename = fileWrapper.getFile().getName();

            if ("bin".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "bin"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("random_100.bin".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "bin/random_100.bin"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("random_1024.bin".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "bin/random_1024.bin"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("zero_100.bin".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "bin/zero_100.bin"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());


            } else if ("img".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "img"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("black.jpg".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "img/black.jpg"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("gradiant.jpg".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "img/gradiant.jpg"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("white.jpg".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "img/white.jpg"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());


            } else if ("root.txt".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "root.txt"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());
                Assert.assertTrue(String.format("The file %s does not exist.", fileWrapper.getFile()), fileWrapper.getFile().exists());

            } else {
                Assert.fail(String.format("Unexpected filename: '%s'", filename));
            }
        }
    }

    @Test
    public void testListFilesFilterRecursivelyWithoutS3() throws Exception {
        URL ioFileUrl = FileWrapperTest.class.getClassLoader().getResource("bucket_files");
        File ioFile = new File(ioFileUrl.toURI());
        FileWrapper rootFileWrapper = new FileWrapper((URI)null, ioFile);

        List<FileWrapper> fileWrapperList = rootFileWrapper.listFiles(null, new FilenameFilter() {
            @Override
            public boolean accept(File dir, String filename) {
                return filename.contains("n");
            }
        }, true);

        Assert.assertNotNull("The file list is null", fileWrapperList);

        for (FileWrapper fileWrapper : fileWrapperList) {
            String filename = fileWrapper.getFile().getName();

            if ("bin".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "bin"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("random_100.bin".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "bin/random_100.bin"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("random_1024.bin".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "bin/random_1024.bin"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("zero_100.bin".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "bin/zero_100.bin"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());

            } else if ("gradiant.jpg".equals(filename)) {
                Assert.assertEquals("The file was not as expected", new File(ioFile, "img/gradiant.jpg"), fileWrapper.getFile());
                Assert.assertNull("The file wrapper is associated with a s3URI.", fileWrapper.getS3URI());


            } else {
                Assert.fail(String.format("Unexpected filename: '%s'", filename));
            }
        }

        Assert.assertEquals("Wrong number of files in the bucket.", 5, fileWrapperList.size());
    }



    /**
     * Upload a file to S3, then download it to see if it has changed.
     * NOTE: The resource file "aws-credentials.properties" must be set before running this test.
     * @throws Exception If something goes wrong...
     */
    @Test
    public void testListFilesInFolder() throws Exception {
        AmazonS3URI s3Uri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "img/");
        File ioFile = new File("/tmp/s3wrapper/FileWrapper/img");
        FileWrapper imgFileWrapper = new FileWrapper(s3Uri, ioFile);

        // Delete the folder on disk if it already exists
        FileUtils.deleteDirectory(ioFile);

        try (S3Client client = super.openS3Client()) {
            super.setupBucket(client);

            List<FileWrapper> fileWrapperList = imgFileWrapper.listFiles(client);


            Assert.assertNotNull("The file list is null", fileWrapperList);

            for (FileWrapper fileWrapper : fileWrapperList) {
                String filename = fileWrapper.getFile().getName();


                if ("black.jpg".equals(filename)) {
                    Assert.assertEquals("The file was not as expected", new File(ioFile, "black.jpg"), fileWrapper.getFile());
                    Assert.assertEquals("The file was not as expected", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "img/black.jpg"), fileWrapper.getS3URI());

                } else if ("gradiant.jpg".equals(filename)) {
                    Assert.assertEquals("The file was not as expected", new File(ioFile, "gradiant.jpg"), fileWrapper.getFile());
                    Assert.assertEquals("The file was not as expected", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "img/gradiant.jpg"), fileWrapper.getS3URI());

                } else if ("white.jpg".equals(filename)) {
                    Assert.assertEquals("The file was not as expected", new File(ioFile, "white.jpg"), fileWrapper.getFile());
                    Assert.assertEquals("The file was not as expected", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "img/white.jpg"), fileWrapper.getS3URI());


                } else {
                    Assert.fail(String.format("Unexpected filename: '%s'", filename));
                }
            }

            Assert.assertEquals("Wrong number of files in the 'img' folder.", 3, fileWrapperList.size());
        }

        // Cleanup at the end of the test
        FileUtils.deleteDirectory(ioFile);
    }

    /**
     * Upload a file to S3, then download it to see if it has changed.
     * NOTE: The resource file "aws-credentials.properties" must be set before running this test.
     * @throws Exception If something goes wrong...
     */
    @Test
    public void testListFilesFilterRecursively() throws Exception {
        AmazonS3URI s3Uri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID);
        File ioFile = new File("/tmp/s3wrapper/FileWrapper");
        FileWrapper rootFileWrapper = new FileWrapper(s3Uri, ioFile);

        // Delete the folder on disk if it already exists
        FileUtils.deleteDirectory(ioFile);

        try (S3Client client = super.openS3Client()) {
            super.setupBucket(client);

            // NOTE: listFiles recursive do NOT list folders (it's a "feature" of the S3 library)
            List<FileWrapper> fileWrapperList = rootFileWrapper.listFiles(client, new FilenameFilter() {
                @Override
                public boolean accept(File dir, String filename) {
                    return filename.contains("n");
                }
            }, true);


            Assert.assertNotNull("The file list is null", fileWrapperList);

            for (FileWrapper fileWrapper : fileWrapperList) {
                String filename = fileWrapper.getFile().getName();


                if ("random_100.bin".equals(filename)) {
                    Assert.assertEquals("The file was not as expected", new File(ioFile, "bin/random_100.bin"), fileWrapper.getFile());
                    Assert.assertEquals("The file was not as expected", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "bin/random_100.bin"), fileWrapper.getS3URI());

                } else if ("random_1024.bin".equals(filename)) {
                    Assert.assertEquals("The file was not as expected", new File(ioFile, "bin/random_1024.bin"), fileWrapper.getFile());
                    Assert.assertEquals("The file was not as expected", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "bin/random_1024.bin"), fileWrapper.getS3URI());

                } else if ("zero_100.bin".equals(filename)) {
                    Assert.assertEquals("The file was not as expected", new File(ioFile, "bin/zero_100.bin"), fileWrapper.getFile());
                    Assert.assertEquals("The file was not as expected", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "bin/zero_100.bin"), fileWrapper.getS3URI());

                } else if ("gradiant.jpg".equals(filename)) {
                    Assert.assertEquals("The file was not as expected", new File(ioFile, "img/gradiant.jpg"), fileWrapper.getFile());
                    Assert.assertEquals("The file was not as expected", S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID, "img/gradiant.jpg"), fileWrapper.getS3URI());


                } else {
                    Assert.fail(String.format("Unexpected filename: '%s'", filename));
                }
            }

            Assert.assertEquals("Wrong number of files in the bucket.", 4, fileWrapperList.size());
        }

        // Cleanup at the end of the test
        FileUtils.deleteDirectory(ioFile);
    }


    /**
     * Upload a file to S3, then download it to see if it has changed.
     * NOTE: The resource file "aws-credentials.properties" must be set before running this test.
     * @throws Exception If something goes wrong...
     */
    @Test
    public void testListFiles() throws Exception {
        AmazonS3URI s3Uri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID);
        File ioFile = new File("/tmp/s3wrapper/FileWrapper");
        FileWrapper rootFileWrapper = new FileWrapper(s3Uri, ioFile);

        // Delete the folder on disk if it already exists
        FileUtils.deleteDirectory(ioFile);

        try (S3Client client = super.openS3Client()) {
            super.setupBucket(client);

            List<FileWrapper> fileWrapperList = rootFileWrapper.listFiles(client);
            Assert.assertNotNull("The file list is null", fileWrapperList);
            Assert.assertEquals("Wrong number of files in the bucket.", 3, fileWrapperList.size());

            for (FileWrapper fileWrapper : fileWrapperList) {
                String filename = fileWrapper.getFile().getName();

                if ("bin".equals(filename)) {
                    Assert.assertEquals("The file was not as expected", new File(ioFile, "bin"), fileWrapper.getFile());
                    // List files in "bin"
                    List<FileWrapper> binFileWrapperList = fileWrapper.listFiles(client);
                    Assert.assertNotNull("The 'bin' file list is null", binFileWrapperList);
                    Assert.assertEquals("Wrong number of files in the 'bin'.", 3, binFileWrapperList.size());

                    // Check files in "bin"
                    for (FileWrapper binFileWrapper : binFileWrapperList) {
                        String binFilename = binFileWrapper.getFile().getName();

                        if ("random_100.bin".equals(binFilename)) {
                            Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/random_100.bin"), binFileWrapper.getFile());

                        } else if ("random_1024.bin".equals(binFilename)) {
                            Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/random_1024.bin"), binFileWrapper.getFile());

                        } else if ("zero_100.bin".equals(binFilename)) {
                            Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/zero_100.bin"), binFileWrapper.getFile());

                            // Try to download the "bin/zero_100.bin" file
                            Assert.assertFalse(String.format("The bin file %s already exists.", binFileWrapper.getFile()), binFileWrapper.getFile().exists());
                            File downloadedFile = binFileWrapper.downloadFile(client);
                            Assert.assertEquals("The downloaded bin file is different from expectation.", binFileWrapper.getFile(), downloadedFile);
                            Assert.assertTrue(String.format("The bin file %s was not downloaded.", binFileWrapper.getFile()), binFileWrapper.getFile().exists());

                        } else {
                            Assert.fail(String.format("Unexpected file key in 'bin': '%s'", binFilename));
                        }
                    }

                } else if ("img".equals(filename)) {
                    Assert.assertEquals("The file was not as expected", new File(ioFile, "img"), fileWrapper.getFile());

                } else if ("root.txt".equals(filename)) {
                    Assert.assertEquals("The file was not as expected", new File(ioFile, "root.txt"), fileWrapper.getFile());

                    // Try to download the "root.txt" file
                    Assert.assertFalse(String.format("The file %s already exists.", fileWrapper.getFile()), fileWrapper.getFile().exists());
                    File downloadedFile = fileWrapper.downloadFile(client);
                    Assert.assertEquals("The downloaded file is different from expectation.", fileWrapper.getFile(), downloadedFile);
                    Assert.assertTrue(String.format("The file %s was not downloaded.", fileWrapper.getFile()), fileWrapper.getFile().exists());

                } else {
                    Assert.fail(String.format("Unexpected filename: '%s'", filename));
                }
            }
        }

        // Cleanup at the end of the test
        FileUtils.deleteDirectory(ioFile);
    }

    /**
     * Check if the download (file to file) works as expected.
     */
    @Test
    public void testDownloadLocalFile() throws Exception {
        try (S3Client s3Client = super.openS3Client()) {
            // 1 MB file
            URI source = FileWrapperTest.class.getClassLoader().getResource("bucket_files/bin/random_1024.bin").toURI();
            File destinationDir = Files.createTempDirectory("s3wrapper_").toFile();
            File destination = new File(destinationDir, "random_1024.bin");

            FileWrapper fileWrapper = new FileWrapper(source, destination);

            fileWrapper.downloadFile(s3Client);

            Assert.assertTrue("Destination file doesn't exists", destination.exists());
            Assert.assertEquals("Wrong destination file size", 1024, destination.length());
        }
    }

    /**
     * Check if the download (S3 to file) works as expected.
     */
    @Test
    public void testDownloadS3File() throws Exception {
        try (S3Client s3Client = super.openS3Client()) {
            // 1 kB file
            URI source = new URI(String.format("s3://%s/bin/random_1024.bin", S3_BUCKET_ID));
            File destinationDir = Files.createTempDirectory("s3wrapper_").toFile();
            File destination = new File(destinationDir, "random_1024.bin");

            FileWrapper fileWrapper = new FileWrapper(source, destination);

            fileWrapper.downloadFile(s3Client);

            Assert.assertTrue("Destination file doesn't exists", destination.exists());
            Assert.assertEquals("Wrong destination file size", 1024, destination.length());
        }
    }

    /**
     * Check if the download (S3 to file) works as expected.
     */
    @Test
    public void testReDownloadS3File() throws Exception {
        File destinationDir = Files.createTempDirectory("s3wrapper_").toFile();
        File destination = new File(destinationDir, "random_1024.bin");

        try (S3Client s3Client = super.openS3Client()) {
            // 100 B file
            URI smallSource = FileWrapperTest.class.getClassLoader().getResource("bucket_files/bin/random_100.bin").toURI();

            // Download a 100B file where the 1kB file will be.
            FileWrapper smallFileWrapper = new FileWrapper(smallSource, destination);
            smallFileWrapper.downloadFile(s3Client);

            Assert.assertTrue("Destination file doesn't exists", destination.exists());
            Assert.assertEquals("Wrong destination file size", 100, destination.length());


            // Ensure the file won't get re-downloaded (it's not outdated)
            // Wait 1 second, to make a difference in file lastmodified if it's changed
            Thread.sleep(1000);

            long lastmodifiedBefore = destination.lastModified();
            smallFileWrapper.downloadFile(s3Client);
            long lastmodifiedAfter = destination.lastModified();

            Assert.assertEquals("The destination file was unnecessarily re-downloaded", lastmodifiedBefore, lastmodifiedAfter);


            // 1 kB file
            URI source = new URI(String.format("s3://%s/bin/random_1024.bin", S3_BUCKET_ID));

            // Download a 1kB file. The library should be able to notice the file size on disk is different,
            // therefore attempt to re-download the file.
            FileWrapper fileWrapper = new FileWrapper(source, destination);
            fileWrapper.downloadFile(s3Client);

            Assert.assertTrue("Destination file doesn't exists", destination.exists());
            Assert.assertEquals("Wrong destination file size", 1024, destination.length());
        }
    }

    /**
     * -- Manual test --
     * Check if the download (S3 to file) thrown an IOException when out of disk space.
     * Can be altered to do a manual S3 to file test (should behave the same).
     * Create a fake disk (in file)
     * Attempt to download a file larger than the space in that disk, to see if it's throwing an IOException
     *
     * Expecting: java.io.IOException: No space left on device
     */
    @Test(expected = IOException.class)
    @Ignore
    public void testDownloadFileNoDiskSpace() throws Exception {
        String instructions = String.format(
                 "Instructions to create the fake disk in /tmp:%n" +
                 "    $ dd if=/dev/zero bs=1M count=10 of=/tmp/fakedisk.img%n" +
                 "    $ /sbin/fdisk /tmp/fakedisk.img%n" +
                 "    $ sudo losetup --partscan --show --find /tmp/fakedisk.img%n" +
                 "    $ sudo mkfs -t ext4 /dev/loop0p1%n" +
                 "    $ mkdir /tmp/fakedisk%n" +
                 "    $ sudo mount /dev/loop0p1 /tmp/fakedisk%n" +
                 "    $ sudo mkdir /tmp/fakedisk/download%n" +
                 "    $ sudo chmod 777 /tmp/fakedisk/download%n" +
                 "    ... run the test%n" +
                 "    $ sudo umount /tmp/fakedisk%n" +
                 "    $ sudo losetup -d /dev/loop0");

        try (S3Client s3Client = super.openS3Client()) {
            // Any file larger than 10 MB
            URI source = new URI("s3://aims-ereefs-public-test/ncanimate/products/products__ncanimate__ereefs__gbr4_v2__temp-wind-salt-current_hourly/products__ncanimate__ereefs__gbr4_v2__temp-wind-salt-current_hourly_video_monthly_2010-09_qld_-1.5.mp4");

            File destinationDir = new File("/tmp/fakedisk/download");
            Assert.assertTrue(String.format("The fake disk doesn't exists. Create the fake disk before running this test.%n%n%s", instructions), destinationDir.isDirectory());
            File destination = new File(destinationDir, "gbr4_v2_temp-wind-salt-current_2010-09_qld_-1.5.mp4");
            if (destination.exists()) {
                Assert.assertTrue(String.format("Could not delete the destination file: %s", destination), destination.delete());
            }

            FileWrapper fileWrapper = new FileWrapper(source, destination);

            boolean crashed = false;
            try {
                fileWrapper.downloadFile(s3Client);
            } catch(IOException ex) {
                crashed = true;
            }
            Assert.assertTrue("The download didn't throw an IOException (disk full)", crashed);

            // It should also crash the second time
            fileWrapper.downloadFile(s3Client);
            Assert.fail("The download should have failed. There is not enough space on disk.");
        }
    }

    @Test
    @Ignore
    public void testFileAnomaly() throws Exception {
        URI source = new URI("s3://aims-ereefs-public-test/ncanimate/products/products__ncanimate__ereefs__gbr4_bgc_924__chl-a-sum_din_tss_daily/products__ncanimate__ereefs__gbr4_bgc_924__chl-a-sum_din_tss_daily_video_yearly_2014_wet-tropics_-1.5.wmv");

        File destinationDir = Files.createTempDirectory("s3wrapper_").toFile();
        File destination = new File(destinationDir, "wet-tropics_-1.5.wmv");

        try (S3Client s3Client = super.openS3Client()) {
            FileWrapper fileWrapper = new FileWrapper(source, destination);

            Long lastModified = fileWrapper.getLastModified(s3Client);

            System.out.println("LAST MODIFIED: " + lastModified);
        }
    }
}
