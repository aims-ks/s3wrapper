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

import au.gov.aims.aws.s3.S3Utils;
import au.gov.aims.aws.s3.entity.S3Bucket;
import au.gov.aims.aws.s3.entity.S3ClientWrapper;
import au.gov.aims.aws.s3.entity.S3File;
import au.gov.aims.aws.s3.entity.S3List;
import org.apache.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UploadManager {
    private static final Logger LOGGER = Logger.getLogger(UploadManager.class);
    private static final int MB = 1024 * 1024;
    private static final int S3_ATTEMPT = 5;

    // S3 have a 5 GB limit for file upload in a single request.
    // Larger files needs to be uploaded by chunks (minimum file size of 5 MB).
    // The following limit is used to determine if a file should be uploaded at once
    // or using the multithreaded multipart upload.
    //   IMPORTANT: Must be between 5 MB and 5 GB
    private static final long S3_MULTIPART_UPLOAD_FILESIZE_THRESHOLD = 500L * MB;

    // Set the size of upload parts.
    // - Small parts will result in large amount of requests, increasing S3 PUT cost.
    // - Large parts may result in less concurrent upload and larger retry when a part fail (?).
    // Default: 5 MB
    //   TransferManagerConfiguration.DEFAULT_MINIMUM_UPLOAD_PART_SIZE = 5 * MB;
    private static final long S3_MULTIPART_UPLOAD_PART_SIZE = 100L * MB;

    public static S3List upload(S3ClientWrapper client, File sourceFile, S3Uri destinationUri) throws IOException, InterruptedException {
        String bucket = destinationUri.bucket().orElse(null);
        if (!BucketManager.bucketExists(client, bucket)) {
            throw new IOException(String.format("Bucket %s doesn't exist.", bucket));
        }

        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                return UploadManager.rawUpload(client, sourceFile, destinationUri);
            } catch(Throwable ex) {
                LOGGER.warn(String.format("Error occurred while trying to upload the file %s on S3: %s. Attempting to reconnect.",
                        sourceFile, destinationUri), ex);
                client.reconnect();
            }
        }
        // Try a last time, to throw the exception
        return UploadManager.rawUpload(client, sourceFile, destinationUri);
    }

    private static S3List rawUpload(S3ClientWrapper client, File sourceFile, S3Uri destinationUri) throws IOException, InterruptedException {
        S3List s3List;

        S3TransferManager transferManager = null;
        try {
            // Code sample:
            // https://docs.aws.amazon.com/AmazonS3/latest/dev/HLuploadFileJava.html
            transferManager = S3TransferManager.builder()
                .s3Client(client.getS3AsyncClient())
                .build();

            S3Bucket bucket = new S3Bucket(destinationUri.bucket().orElse(null));
            boolean bucketIsPublic = bucket.isPublic(client);

            long startTime = System.currentTimeMillis();

            s3List = UploadManager.upload(transferManager, sourceFile, destinationUri, bucketIsPublic);

            long endTime = System.currentTimeMillis();

            s3List.setExecutionTime(endTime - startTime);
        } finally {
            if (transferManager != null) {
                try {
                    transferManager.close();
                } catch(Exception ex) {
                    LOGGER.error("Could not close the S3 transfer manager.", ex);
                }
            }
        }

        return s3List;
    }


    // Internal upload (recursive)
    private static S3List upload(S3TransferManager transferManager, File sourceFile, S3Uri destinationUri, boolean bucketIsPublic) throws IOException, InterruptedException {
        S3List s3List = new S3List();

        String bucket = destinationUri.bucket().orElse(null);
        String key = destinationUri.key().orElse(null);

        if (sourceFile.isDirectory()) {
            // Upload a directory (recursive)

            File[] childFiles = sourceFile.listFiles();

            if (childFiles != null && childFiles.length > 0) {
                S3Uri childDestinationUri =
                    S3Utils.getS3URI(bucket, key, sourceFile.getName() + "/");

                for (File childFile : childFiles) {
                    s3List.putAll(
                        UploadManager.upload(transferManager, childFile, childDestinationUri, bucketIsPublic));
                }
            }

        } else if (sourceFile.isFile()) {
            // Upload a single file

            if (sourceFile.length() <= 0) {
                throw new IOException(String.format("The source file '%s' is empty.", sourceFile.getAbsolutePath()));
            }

            if (S3Utils.getFilename(destinationUri) == null) {
                destinationUri = S3Utils.getS3URI(bucket, key, sourceFile.getName());
            }


            // File too big, uploading it in chunks
            // http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html

            Map<String, String> customMetadata = new HashMap<String, String>();
            customMetadata.put(S3File.USER_METADATA_LAST_MODIFIED_KEY, ""+sourceFile.lastModified());

            PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .metadata(customMetadata);

            if (bucketIsPublic) {
                putObjectRequestBuilder.acl(ObjectCannedACL.PUBLIC_READ);
            }

            PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();

            UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                .putObjectRequest(putObjectRequest)
                .source(sourceFile)
                .build();

            LOGGER.debug(String.format("Uploading file %s (%d MB) to %s",
                sourceFile,
                sourceFile.length() / MB,
                destinationUri));

            // TransferManager processes all transfers asynchronously,
            // so this call returns immediately.
            FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);

            LOGGER.debug("Upload started...");

            // Wait for the upload to finish before continuing.
            fileUpload.completionFuture().join();

            LOGGER.debug("Upload completed.");

            S3File s3File = new S3File(destinationUri);
            s3File.setLocalFile(sourceFile);

            s3List.putFile(s3File);

        } else {
            throw new IOException(String.format("Can not upload the file %s, it's not a normal file.", sourceFile.getAbsolutePath()));
        }

        return s3List;
    }

}
