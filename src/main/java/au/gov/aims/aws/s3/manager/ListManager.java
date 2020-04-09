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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.List;
import java.util.regex.Pattern;

public class ListManager {
    private static final Logger LOGGER = Logger.getLogger(ListManager.class);
    private static final int S3_ATTEMPT = 5;

    public static S3List ls(S3Client client, AmazonS3URI s3Uri) {
        return ListManager.ls(client, s3Uri, null, null, false);
    }
    public static S3List ls(S3Client client, AmazonS3URI s3Uri, boolean recursive) {
        return ListManager.ls(client, s3Uri, null, null, recursive);
    }

    public static S3List ls(S3Client client, AmazonS3URI s3Uri, FilenameFilter filenameFilter) {
        return ListManager.ls(client, s3Uri, filenameFilter, null, false);
    }
    public static S3List ls(S3Client client, AmazonS3URI s3Uri, FilenameFilter filenameFilter, boolean recursive) {
        return ListManager.ls(client, s3Uri, filenameFilter, null, recursive);
    }

    public static S3List ls(S3Client client, AmazonS3URI s3Uri, FileFilter fileFilter) {
        return ListManager.ls(client, s3Uri, null, fileFilter, false);
    }
    public static S3List ls(S3Client client, AmazonS3URI s3Uri, FileFilter fileFilter, boolean recursive) {
        return ListManager.ls(client, s3Uri, null, fileFilter, recursive);
    }

    private static S3List ls(S3Client client, AmazonS3URI s3Uri, FilenameFilter filenameFilter, FileFilter fileFilter, boolean recursive) {
        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                return rawLs(client, s3Uri, filenameFilter, fileFilter, recursive);
            } catch(Throwable ex) {
                LOGGER.warn(String.format("Error occurred while trying to list files on S3: %s. Attempting to reconnect.",
                        s3Uri), ex);
                client.reconnect();
            }
        }
        // Try a last time, to throw the exception
        return rawLs(client, s3Uri, filenameFilter, fileFilter, recursive);
    }

    private static S3List rawLs(S3Client client, AmazonS3URI s3Uri, FilenameFilter filenameFilter, FileFilter fileFilter, boolean recursive) {
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
                .withPrefix(s3Uri.getKey());

        if (!recursive) {
            listObjectsRequest = listObjectsRequest.withDelimiter("/");
        }

        AmazonS3 s3 = client.getS3();

        ObjectListing objectListing = s3.listObjects(listObjectsRequest);

        // See: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3-buckets.html
        boolean listIsTruncated = true;
        while (listIsTruncated) {
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                AmazonS3URI fileS3Uri = S3Utils.getS3URI(s3Uri.getBucket(), objectSummary.getKey());

                boolean selected = false;
                if (filenameFilter != null) {
                    String parentKey = S3Utils.getParentUri(fileS3Uri).getKey();
                    if (parentKey == null) {
                        parentKey = "";
                    }
                    selected = filenameFilter.accept(new File(parentKey), S3Utils.getFilename(fileS3Uri));
                } else if (fileFilter != null) {
                    String fileKey = fileS3Uri.getKey();
                    if (fileKey == null) {
                        fileKey = "";
                    }
                    selected = fileFilter.accept(new File(fileKey));
                } else if (filter != null) {
                    String s3Filename = S3Utils.getFilename(fileS3Uri);
                    if (s3Filename != null) {
                        selected = filter.matcher(s3Filename).matches();
                    }
                } else {
                    selected = true;
                }

                if (selected) {
                    // Can't reconnect S3 client in a middle of a listing...
                    S3Object fullObject = s3.getObject(fileS3Uri.getBucket(), fileS3Uri.getKey());
                    s3List.putFile(new S3File(fileS3Uri, fullObject.getObjectMetadata()));
                }
            }

            // See: https://stackoverflow.com/questions/14653694/listing-just-the-sub-folders-in-an-s3-bucket#answer-14653973
            //   https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/ObjectListing.html#getCommonPrefixes--
            List<String> directories = objectListing.getCommonPrefixes();
            for (String directory : directories) {
                if (!directory.endsWith("/")) {
                    directory += "/";
                }
                AmazonS3URI fileS3Uri = S3Utils.getS3URI(s3Uri.getBucket(), directory);

                boolean selected = false;
                if (filenameFilter != null) {
                    String parentKey = S3Utils.getParentUri(fileS3Uri).getKey();
                    if (parentKey == null) {
                        parentKey = "";
                    }
                    selected = filenameFilter.accept(new File(parentKey), S3Utils.getDirectoryName(fileS3Uri));
                } else if (fileFilter != null) {
                    String fileKey = fileS3Uri.getKey();
                    if (fileKey == null) {
                        fileKey = "";
                    }
                    selected = fileFilter.accept(new File(fileKey));
                } else if (filter != null) {
                    String dirName = S3Utils.getDirectoryName(fileS3Uri);
                    if (dirName != null) {
                        selected = filter.matcher(dirName).matches();
                    }
                } else {
                    selected = true;
                }

                if (selected) {
                    s3List.putDir(new S3File(fileS3Uri));
                }
            }

            listIsTruncated = objectListing.isTruncated();
            if (listIsTruncated) {
                // Can't reconnect S3 client in a middle of a listing...
                objectListing = s3.listNextBatchOfObjects(objectListing);
            }
        }

        long endTime = System.currentTimeMillis();

        s3List.setExecutionTime(endTime - startTime);

        return s3List;
    }
}
