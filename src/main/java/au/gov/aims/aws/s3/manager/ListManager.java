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
import au.gov.aims.aws.s3.entity.S3ClientWrapper;
import au.gov.aims.aws.s3.entity.S3File;
import au.gov.aims.aws.s3.entity.S3List;
import org.apache.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

public class ListManager {
    private static final Logger LOGGER = Logger.getLogger(ListManager.class);
    private static final int S3_ATTEMPT = 5;

    public static S3List ls(S3ClientWrapper client, S3Uri s3Uri) {
        return ListManager.ls(client, s3Uri, null, null, false);
    }
    public static S3List ls(S3ClientWrapper client, S3Uri s3Uri, boolean recursive) {
        return ListManager.ls(client, s3Uri, null, null, recursive);
    }

    public static S3List ls(S3ClientWrapper client, S3Uri s3Uri, FilenameFilter filenameFilter) {
        return ListManager.ls(client, s3Uri, filenameFilter, null, false);
    }
    public static S3List ls(S3ClientWrapper client, S3Uri s3Uri, FilenameFilter filenameFilter, boolean recursive) {
        return ListManager.ls(client, s3Uri, filenameFilter, null, recursive);
    }

    public static S3List ls(S3ClientWrapper client, S3Uri s3Uri, FileFilter fileFilter) {
        return ListManager.ls(client, s3Uri, null, fileFilter, false);
    }
    public static S3List ls(S3ClientWrapper client, S3Uri s3Uri, FileFilter fileFilter, boolean recursive) {
        return ListManager.ls(client, s3Uri, null, fileFilter, recursive);
    }

    private static S3List ls(S3ClientWrapper client, S3Uri s3Uri, FilenameFilter filenameFilter, FileFilter fileFilter, boolean recursive) {
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

    private static S3List rawLs(S3ClientWrapper client, S3Uri s3Uri, FilenameFilter filenameFilter, FileFilter fileFilter, boolean recursive) {
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


        String bucket = s3Uri.bucket().orElse(null);
        String key = s3Uri.key().orElse(null);

        String continuationToken = null;
        do {

            ListObjectsV2Request.Builder pageBuilder = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(key);
            if (continuationToken != null) {
                pageBuilder.continuationToken(continuationToken);
            }

            if (!recursive) {
                pageBuilder.delimiter("/");
            }

            ListObjectsV2Request pageRequest = pageBuilder.build();

            ListObjectsV2Response page = ListManager.rawLsPage(client, pageRequest);

            for (S3Object s3Object : page.contents()) {
                S3Uri fileS3Uri = S3Utils.getS3URI(bucket, s3Object.key());

                boolean selected = false;
                if (filenameFilter != null) {
                    String parentKey = S3Utils.getParentUri(fileS3Uri).key().orElse("");
                    selected = filenameFilter.accept(new File(parentKey), S3Utils.getFilename(fileS3Uri));
                } else if (fileFilter != null) {
                    String fileKey = fileS3Uri.key().orElse("");
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
                    s3List.putFile(new S3File(fileS3Uri));
                }
            }

            for (CommonPrefix directoryObject : page.commonPrefixes()) {
                String directory = directoryObject.prefix();
                if (!directory.endsWith("/")) {
                    directory += "/";
                }

                S3Uri directoryS3Uri = S3Utils.getS3URI(bucket, directory);

                boolean selected = false;
                if (filenameFilter != null) {
                    String parentKey = S3Utils.getParentUri(directoryS3Uri).key().orElse(null);
                    if (parentKey == null) {
                        parentKey = "";
                    }
                    selected = filenameFilter.accept(new File(parentKey), S3Utils.getDirectoryName(directoryS3Uri));
                } else if (fileFilter != null) {
                    String fileKey = directoryS3Uri.key().orElse(null);
                    if (fileKey == null) {
                        fileKey = "";
                    }
                    selected = fileFilter.accept(new File(fileKey));
                } else if (filter != null) {
                    String dirName = S3Utils.getDirectoryName(directoryS3Uri);
                    if (dirName != null) {
                        selected = filter.matcher(dirName).matches();
                    }
                } else {
                    selected = true;
                }

                if (selected) {
                    s3List.putDir(new S3File(directoryS3Uri));
                }

            }

            continuationToken = page.nextContinuationToken();
        } while (continuationToken != null);

        long endTime = System.currentTimeMillis();

        s3List.setExecutionTime(endTime - startTime);

        return s3List;
    }

    private static ListObjectsV2Response rawLsPage(S3ClientWrapper client, ListObjectsV2Request pageRequest) {
        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                return client.getS3Client().listObjectsV2(pageRequest);
            } catch(Throwable ex) {
                LOGGER.warn("Error occurred while listing a page of S3 files. Attempting to reconnect.", ex);
                client.reconnect();
            }
        }
        // Try a last time, to throw the exception
        return client.getS3Client().listObjectsV2(pageRequest);
    }
}
