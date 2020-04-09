package au.gov.aims.aws.s3.manager;

import au.gov.aims.aws.s3.S3TestBase;
import au.gov.aims.aws.s3.entity.S3Client;
import au.gov.aims.aws.s3.entity.S3File;
import au.gov.aims.aws.s3.entity.S3List;
import com.amazonaws.services.s3.AmazonS3URI;
import org.junit.Ignore;
import org.junit.Test;

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

        try (S3Client client = super.openS3Client()) {
            S3List list = ListManager.ls(client, new AmazonS3URI(s3UriStr), false);

            for (Map.Entry<String, S3File> dirEntry : list.getDirs().entrySet()) {
                System.out.println("DIR: " + dirEntry.getKey());
            }
            for (Map.Entry<String, S3File> fileEntry : list.getFiles().entrySet()) {
                System.out.println("FILE: " + fileEntry.getKey());
            }
        }
    }
}
