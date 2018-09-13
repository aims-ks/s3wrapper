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
import au.gov.aims.aws.s3.entity.S3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.Bucket;

public class BucketManager {

	public static S3Bucket create(S3Client client, AmazonS3URI s3Uri) throws Exception {
		return BucketManager.create(client, s3Uri.getBucket());
	}

	public static S3Bucket create(S3Client client, String bucketId) throws Exception {
		// Remove trailing white spaces
		bucketId = bucketId == null ? null : bucketId.trim();

		if (bucketId == null || bucketId.isEmpty()) {
			throw new IllegalArgumentException("Can not create bucket: Bucket ID is null or empty");
		}

		if (client.getS3().doesBucketExistV2(bucketId)) {
			throw new BucketAlreadyExistsException(String.format("Bucket %s already exists.", bucketId));
		}


		long startTime = System.currentTimeMillis();

		Bucket bucket = client.getS3().createBucket(bucketId);

		long endTime = System.currentTimeMillis();


		S3Bucket s3Bucket = new S3Bucket(bucketId);
		s3Bucket.setExecutionTime(endTime - startTime);
		s3Bucket.setCreationTime(bucket.getCreationDate());

		return s3Bucket;
	}

	public static class BucketAlreadyExistsException extends Exception {
		public BucketAlreadyExistsException(String message) {
			super(message);
		}
	}
}

