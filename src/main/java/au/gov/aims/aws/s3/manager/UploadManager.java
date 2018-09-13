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
import au.gov.aims.aws.s3.entity.S3Client;
import au.gov.aims.aws.s3.entity.S3File;
import au.gov.aims.aws.s3.entity.S3List;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class UploadManager {
	private static final Logger LOGGER = Logger.getLogger(UploadManager.class);

	// S3 have a 5 GB limit for file upload in a single request.
	// Larger files needs to be uploaded by chunks (minimum file size of 5 MB).
	// The following limit is used to determine if a file should be uploaded at once
	// or using the multi threaded multipart upload.
	//   IMPORTANT: Must be between 5 MB and 5 GB
	private static final long S3_MULTIPART_UPLOAD_FILESIZE_THRESHOLD = 500L * Constants.MB;

	// Set the size of upload parts.
	// - Small parts will results in large amount of requests, increasing S3 PUT cost.
	// - Large parts may results in less concurrent upload and larger retry when a part fail (?).
	// Default: 5 MB
	//   TransferManagerConfiguration.DEFAULT_MINIMUM_UPLOAD_PART_SIZE = 5 * MB;
	private static final long S3_MULTIPART_UPLOAD_PART_SIZE = 100L * Constants.MB;

	public static S3List upload(S3Client client, File sourceFile, AmazonS3URI destinationUri) throws IOException, InterruptedException {
		S3List s3List;

		TransferManager transferManager = null;
		try {
			if (!client.getS3().doesBucketExistV2(destinationUri.getBucket())) {
				throw new IOException(String.format("Bucket %s doesn't exist.", destinationUri.getBucket()));
			}

			// Code sample:
			// https://docs.aws.amazon.com/AmazonS3/latest/dev/HLuploadFileJava.html
			transferManager = TransferManagerBuilder.standard()
				.withS3Client(client.getS3())
				.withMinimumUploadPartSize(UploadManager.S3_MULTIPART_UPLOAD_PART_SIZE)
				.withMultipartUploadThreshold(UploadManager.S3_MULTIPART_UPLOAD_FILESIZE_THRESHOLD)
				.build();

			S3Bucket bucket = new S3Bucket(destinationUri.getBucket());
			boolean bucketIsPublic = bucket.isPublic(client);


			long startTime = System.currentTimeMillis();

			s3List = UploadManager.upload(transferManager, sourceFile, destinationUri, bucketIsPublic);

			long endTime = System.currentTimeMillis();


			s3List.setExecutionTime(endTime - startTime);
		} finally {
			if (transferManager != null) {
				try {
					transferManager.shutdownNow(false);
				} catch(Exception ex) {
					LOGGER.error("Could not shutdown the transfer manager.", ex);
				}
			}
		}

		return s3List;
	}


	// Internal upload (recursive)
	private static S3List upload(TransferManager transferManager, File sourceFile, AmazonS3URI destinationUri, boolean bucketIsPublic) throws IOException, InterruptedException {
		S3List s3List = new S3List();

		if (sourceFile.isDirectory()) {
			// Upload a directory (recursive)

			// Can't use "TransferManager.uploadDirectory", each file needs custom metadata.
			// MultipleFileUpload multipleFileUpload = transferManager.uploadDirectory(
			//   destinationUri.getBucketId(),
			//   destinationUri.getAbsolutePath(),
			//   sourceFile,
			//   true);

			File[] childFiles = sourceFile.listFiles();

			if (childFiles != null && childFiles.length > 0) {
				// Make sure the destination is a directory
				String destinationKey = destinationUri.getKey();
				if (destinationKey == null) {
					destinationKey = "";
				}
				if (!destinationKey.endsWith("/")) {
					destinationKey += "/";
				}

				AmazonS3URI childDestinationUri = new AmazonS3URI("s3://" + destinationUri.getBucket() + "/" + destinationKey + sourceFile.getName() + "/");

				for (File childFile : childFiles) {
					s3List.addAll(
						UploadManager.upload(transferManager, childFile, childDestinationUri, bucketIsPublic));
				}
			}

		} else if (sourceFile.isFile()) {
			// Upload a single file

			if (sourceFile.length() <= 0) {
				throw new IOException(String.format("The source file '%s' is empty.", sourceFile.getAbsolutePath()));
			}

			String filename = S3Utils.getFilename(destinationUri);
			if (filename == null) {
				destinationUri = new AmazonS3URI("s3://" + destinationUri.getBucket() + "/" + destinationUri.getKey() + sourceFile.getName());
			}


			// File too big, uploading it in chunks
			// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
			LOGGER.debug(String.format("Uploading file '%s' %d MB to '%s'",
				sourceFile.getAbsolutePath(),
				sourceFile.length() / Constants.MB,
				destinationUri.toString()));

			Map<String, String> customMetadata = new HashMap<String, String>();
			customMetadata.put(S3File.USER_METADATA_LAST_MODIFIED_KEY, ""+sourceFile.lastModified());

			ObjectMetadata fileMetadata = new ObjectMetadata();
			// NOTE: ObjectMetadata.setLastModified is for "internal use only".
			//   It can NOT be used to set the file lastModified date.
			//   fileMetadata.setLastModified(new Date(sourceFile.lastModified()));
			fileMetadata.setUserMetadata(customMetadata);

			PutObjectRequest putRequest = new PutObjectRequest(
				destinationUri.getBucket(),
				destinationUri.getKey(),
				sourceFile).withMetadata(fileMetadata);

			// Make the file public if it's saved in a public bucket
			if (bucketIsPublic) {
				putRequest = putRequest.withCannedAcl(CannedAccessControlList.PublicRead);
			}

			// TransferManager processes all transfers asynchronously,
			// so this call returns immediately.
			Upload upload = transferManager.upload(putRequest);

			LOGGER.debug("Upload started...");

			// We can show progress... upload.getProgress();

			// Optionally, wait for the upload to finish before continuing.
			upload.waitForCompletion();

			LOGGER.debug("Upload completed.");


			S3File s3File = new S3File(destinationUri);

			s3List.addFile(s3File);

		} else {
			throw new IOException(String.format("Can not upload the file '%s', it's not a normal file.", sourceFile.getAbsolutePath()));
		}

		return s3List;
	}

}
