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
import au.gov.aims.aws.s3.entity.S3List;
import au.gov.aims.aws.s3.manager.BucketManager;
import au.gov.aims.aws.s3.manager.UploadManager;
import com.amazonaws.services.s3.AmazonS3URI;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.URL;

public class S3TestBase {
	private static final Logger LOGGER = Logger.getLogger(S3TestBase.class);
	protected static final String S3_BUCKET_ID = "aims-s3wrapper-junit-test";

	protected S3Client openS3Client() throws Exception {
		return S3Client.parse(PropertiesLoader.load("aws-credentials.properties"));
	}

	protected void setupBucket(S3Client client) throws Exception {
		// Create the S3 JUnit test bucket, if it doesn't already exist
		try {
			BucketManager.create(client, S3TestBase.S3_BUCKET_ID);

			// Copy the content of the bucket_files resource folder into the S3 bucket
			URL bucketFilesUrl = S3TestBase.class.getClassLoader().getResource("bucket_files/");

			File bucketFilesFolder = new File(bucketFilesUrl.toURI());
			S3List uploadedFiles = new S3List();
			AmazonS3URI bucketURI = S3Utils.getS3URI(S3_BUCKET_ID);
			for (File bucketFilesFile : bucketFilesFolder.listFiles()) {
				uploadedFiles.putAll(UploadManager.upload(client, bucketFilesFile, bucketURI));
			}
			LOGGER.info(String.format("%n" +
				"############# TEST BUCKET CREATED #############%n" +
				"Bucket ID: %s%n" +
				"Files: %s%n" +
				"###############################################", S3TestBase.S3_BUCKET_ID, uploadedFiles.toString()));

		} catch(BucketManager.BucketAlreadyExistsException ex) {
			LOGGER.debug(String.format("The bucket %s already exists.", S3TestBase.S3_BUCKET_ID), ex);
		}
	}
}
