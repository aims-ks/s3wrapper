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

import au.gov.aims.aws.s3.entity.S3Bucket;
import au.gov.aims.aws.s3.entity.S3ClientWrapper;
import org.apache.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class BucketManager {
    private static final Logger LOGGER = Logger.getLogger(BucketManager.class);
    private static final int S3_ATTEMPT = 5;

    public static S3Bucket create(S3ClientWrapper client, S3Uri s3Uri) throws Exception {
        return BucketManager.create(client, s3Uri.bucket().orElse(null));
    }

    public static S3Bucket create(S3ClientWrapper client, String bucketId) throws Exception {
        // Remove trailing white spaces
        bucketId = bucketId == null ? null : bucketId.trim();

        if (bucketId == null || bucketId.isEmpty()) {
            throw new IllegalArgumentException("Can not create bucket: Bucket ID is null or empty");
        }

        if (BucketManager.bucketExists(client, bucketId)) {
            throw new BucketAlreadyExistsException(String.format("Bucket %s already exists.", bucketId));
        }


        long startTime = System.currentTimeMillis();

        BucketManager.createS3Bucket(client, bucketId);

        long endTime = System.currentTimeMillis();


        S3Bucket s3Bucket = new S3Bucket(bucketId);
        s3Bucket.setExecutionTime(endTime - startTime);

        return s3Bucket;
    }

    public static boolean bucketExists(S3ClientWrapper client, String bucket) {
        HeadBucketRequest request = HeadBucketRequest.builder()
                .bucket(bucket)
                .build();

        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                return BucketManager.internalBucketExists(client, request);
            } catch(Throwable ex) {
                LOGGER.warn(String.format("Error occurred while checking the existence of a bucket on S3: %s. Attempting to reconnect.",
                        bucket), ex);
                client.reconnect();
            }
        }

        // Try for a last time. If it still doesn't work, it will throw an exception.
        return BucketManager.internalBucketExists(client, request);
    }

    private static boolean internalBucketExists(S3ClientWrapper client, HeadBucketRequest request) {
        try {
            client.getS3Client().headBucket(request);
            return true;
        } catch (S3Exception ex) {
            if (ex.statusCode() == 404) {
                return false;
            }
            throw ex;
        }
    }

    private static CreateBucketResponse createS3Bucket(S3ClientWrapper client, String bucket) {
        CreateBucketRequest request = CreateBucketRequest.builder()
            .bucket(bucket)
            .build();

        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                return client.getS3Client().createBucket(request);
            } catch(Throwable ex) {
                LOGGER.warn(String.format("Error occurred while creating a new bucket on S3: %s. Attempting to reconnect.",
                        bucket), ex);
                client.reconnect();
            }
        }

        // Try for a last time. If it still doesn't work, it will throw an exception.
        return client.getS3Client().createBucket(request);
    }

    public static class BucketAlreadyExistsException extends Exception {
        public BucketAlreadyExistsException(String message) {
            super(message);
        }
    }
}

