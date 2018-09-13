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
import au.gov.aims.aws.s3.entity.S3Client;
import au.gov.aims.aws.s3.entity.S3File;
import au.gov.aims.aws.s3.entity.S3List;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.List;
import java.util.regex.Pattern;

public class ListManager {

	public static S3List ls(S3Client client, AmazonS3URI s3Uri) {
		S3List s3List = new S3List();

		long startTime = System.currentTimeMillis();

		String filename = S3Utils.getFilename(s3Uri);
		Pattern filter = null;
		if (S3Utils.isPattern(filename)) {
			filter = S3Utils.toPattern(filename);
			// Remove the "pattern" filename from the S3URI.
			// This will list all file in the parent directory.
			// Those will be filtered using the "filter"
			s3Uri = S3Utils.getParentUri(s3Uri);
		}

		// See: https://www.programcreek.com/java-api-examples/?api=com.amazonaws.services.s3.model.ListObjectsRequest
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
			.withBucketName(s3Uri.getBucket())
			.withPrefix(s3Uri.getKey())
			.withDelimiter("/");
		ObjectListing objectListing = client.getS3().listObjects(listObjectsRequest);

		// See: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3-buckets.html
		boolean listIsTruncated = true;
		while (listIsTruncated) {
			for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
				AmazonS3URI fileS3Uri = new AmazonS3URI("s3://" + s3Uri.getBucket() + "/" + objectSummary.getKey());

				boolean selected = filter == null || filter.matcher(S3Utils.getFilename(fileS3Uri)).matches();

				if (selected) {
					S3Object fullObject = client.getS3().getObject(fileS3Uri.getBucket(), fileS3Uri.getKey());
					s3List.addFile(new S3File(fileS3Uri, fullObject.getObjectMetadata()));
				}
			}

			listIsTruncated = objectListing.isTruncated();
			if (listIsTruncated) {
				objectListing = client.getS3().listNextBatchOfObjects(objectListing);
			}
		}

		// See: https://stackoverflow.com/questions/14653694/listing-just-the-sub-folders-in-an-s3-bucket#answer-14653973
		//   https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/ObjectListing.html#getCommonPrefixes--
		List<String> directories = objectListing.getCommonPrefixes();
		for (String directory : directories) {
			AmazonS3URI fileS3Uri = new AmazonS3URI("s3://" + s3Uri.getBucket() + "/" + directory);
			s3List.addDir(new S3File(fileS3Uri));
		}

		long endTime = System.currentTimeMillis();

		s3List.setExecutionTime(endTime - startTime);

		return s3List;
	}
}
