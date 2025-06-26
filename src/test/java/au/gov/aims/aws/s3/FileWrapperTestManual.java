package au.gov.aims.aws.s3;

import au.gov.aims.aws.s3.entity.S3ClientWrapper;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;

public class FileWrapperTestManual extends S3TestBase {
    private static final int MB = 1024 * 1024;

    /**
     * This test can be used to verify how the system sees S3 files
     * @throws Exception
     */
    @Test
    @Ignore
    public void testS3File() throws Exception {
        //URI source = new URI("s3://aims-ereefs-public-prod/ncanimate/products/products__ncanimate__ereefs__gbr4_bgc_baseline__chl-a-sum_din_efi_daily/products__ncanimate__ereefs__gbr4_bgc_baseline__chl-a-sum_din_efi_daily_video_yearly_2014_queensland-1_-1.5.mp4");
        //URI source = new URI("s3://aims-ereefs-public-prod/ncanimate/products/products__ncanimate__ereefs__gbr4_bgc_baseline__chl-a-sum_din_efi_daily/products__ncanimate__ereefs__gbr4_bgc_baseline__chl-a-sum_din_efi_daily_video_yearly_2014_south-2_-1.5.mp4");
        URI source = new URI("s3://aims-ereefs-public-prod/ncanimate/products/products__ncanimate__ereefs__gbr4_bgc_baseline__chl-a-sum_din_efi_daily/products__ncanimate__ereefs__gbr4_bgc_baseline__chl-a-sum_din_efi_daily_video_yearly_2014_central-2_-1.5.mp4");

        File destinationDir = Files.createTempDirectory("s3wrapper_").toFile();
        File destination = new File(destinationDir, "wet-tropics_-1.5.wmv");

        try (S3ClientWrapper s3Client = super.openS3Client()) {
            FileWrapper fileWrapper = new FileWrapper(source, destination);

            // Make sure the file exist
            boolean fileExist = fileWrapper.exists(s3Client);
            Assert.assertTrue(
                String.format("The file doesn't exist on S3: %s", fileWrapper.getS3URI().uri()),
                fileExist);

            Long lastModified = fileWrapper.getLastModified(s3Client);

            LocalDate expectedLastModifiedDate = LocalDate.of(2021, 9, 7);
            long expectedLastModifiedLowerBound = expectedLastModifiedDate.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
            long expectedLastModifiedUpperBound = new Date().getTime();
            System.out.println("LAST MODIFIED: " + lastModified);

            Assert.assertNotNull(
                String.format("The file's last modified date is null: %s", fileWrapper.getS3URI().uri()),
                lastModified);
            Assert.assertTrue(
                String.format("The file's last modified date is not in expected range: %s", fileWrapper.getS3URI().uri()),
                lastModified > expectedLastModifiedLowerBound && lastModified < expectedLastModifiedUpperBound);
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

        try (S3ClientWrapper s3Client = super.openS3Client()) {
            // Any file larger than 10 MB
            URI source = new URI("s3://aims-ereefs-public-test/ncanimate/products/products__ncanimate__ereefs__gbr4_v2__temp-wind-salt-current_hourly/products__ncanimate__ereefs__gbr4_v2__temp-wind-salt-current_hourly_video_monthly_2010-09_queensland-1_-1.5.mp4");

            File destinationDir = new File("/tmp/fakedisk/download");
            Assert.assertTrue(String.format("The fake disk doesn't exist. Create the fake disk before running this test.%n%n%s", instructions), destinationDir.isDirectory());
            File destination = new File(destinationDir, "gbr4_v2_temp-wind-salt-current_2010-09_qld_-1.5.mp4");
            if (destination.exists()) {
                Assert.assertTrue(String.format("Could not delete the destination file: %s", destination), destination.delete());
            }

            FileWrapper fileWrapper = new FileWrapper(source, destination);

            // Make sure the file exist
            boolean fileExist = fileWrapper.exists(s3Client);
            Assert.assertTrue(
                String.format("The file doesn't exist on S3: %s", fileWrapper.getS3URI().uri()),
                fileExist);

            // Make sure the file is larger than 10 MB
            Long fileSize = fileWrapper.getS3FileSize(s3Client);
            Assert.assertNotNull(
                String.format("The file size is null: %s", fileWrapper.getS3URI().uri()),
                fileSize);
            Assert.assertTrue(
                String.format("The file size is smaller than 10MB: %s", fileWrapper.getS3URI().uri()),
                fileSize > 10 * MB);

            boolean crashed = false;
            try {
                System.out.printf("Downloading: %s%n", fileWrapper.getS3URI().uri());
                fileWrapper.downloadFile(s3Client);
                System.out.println("Download completed. This should not happen.");
            } catch(IOException ex) {
                crashed = true;
            }
            Assert.assertTrue("The download didn't throw an IOException (disk full)", crashed);

            // It should also crash the second time
            fileWrapper.downloadFile(s3Client);
            Assert.fail("Download completed. The download should have failed: there is not enough space on disk.");
        }
    }
}
