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
package au.gov.aims.aws.s3.entity;

import au.gov.aims.aws.s3.S3Utils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class S3File implements Comparable<S3File> {
    private static final Logger LOGGER = Logger.getLogger(S3File.class);
    private static final int S3_ATTEMPT = 5;

    public static final String USER_METADATA_LAST_MODIFIED_KEY = "lastmodified";

    private final S3Uri s3Uri;
    private final S3Bucket bucket;

    // Metadata
    private boolean metadataFetched = false;
    private Long lastModified;
    private Long fileSize;
    private Long expiration;

    private String eTag;
    private String versionId;

    // Used with upload / download
    private File localFile;

    public S3File(S3Uri s3Uri) {
        this.s3Uri = s3Uri;
        this.bucket = new S3Bucket(s3Uri.bucket().orElse(null));
    }

    public S3File(S3Uri s3Uri, HeadObjectResponse objectMetadata) {
        this(s3Uri);
        if (objectMetadata != null) {
            this.loadMetadata(objectMetadata);
        }
    }

    private void fetchMetadata(S3ClientWrapper client) {
        this.loadMetadata(S3File.getS3ObjectMetadata(client, this.s3Uri));
    }

    private void loadMetadata(HeadObjectResponse objectMetadata) {
        Map<String, String> userMetadata = objectMetadata.metadata();

        // Attempt to get the last modified date from the custom metadata
        Long metadataLastModified = null;
        if (userMetadata != null) {
            String lastModifiedStr = userMetadata.get(S3File.USER_METADATA_LAST_MODIFIED_KEY);
            if (lastModifiedStr != null) {
                metadataLastModified = Long.parseLong(lastModifiedStr);
            }
        }
        // If the last modification date could not be found in the custom metadata, fall back
        // to AWS last modification date.
        if (metadataLastModified == null) {
            this.setLastModified(objectMetadata.lastModified().toEpochMilli());
        } else {
            this.setLastModified(metadataLastModified);
        }

        String expiresString = objectMetadata.expiresString();
        Long expiration = null;
        if (expiresString != null) {
            Instant expirationInstant = Instant.from(DateTimeFormatter.RFC_1123_DATE_TIME.parse(expiresString));
            expiration = expirationInstant.toEpochMilli();
        }
        this.setExpiration(expiration);

        this.setETag(objectMetadata.eTag());
        this.setVersionId(objectMetadata.versionId());
        this.setFileSize(objectMetadata.contentLength());
        this.metadataFetched = true;
    }

    public static boolean fileExists(S3ClientWrapper client, S3Uri sourceUri) {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucket(sourceUri.bucket().orElse(null))
                .key(sourceUri.key().orElse(null))
                .build();

        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                return S3File.internalFileExists(client, request);
            } catch(Throwable ex) {
                LOGGER.warn(String.format("Error occurred while checking the existence of a file on S3: %s. Attempting to reconnect.",
                        sourceUri), ex);
                client.reconnect();
            }
        }
        // Try a last time, to throw the exception
        return S3File.internalFileExists(client, request);
    }

    private static boolean internalFileExists(S3ClientWrapper s3Client, HeadObjectRequest request) {
        try {
            s3Client.getS3Client().headObject(request);
            return true;
        } catch (NoSuchKeyException ex) {
            return false; // Object doesn't exist
        } catch (S3Exception ex) {
            if (ex.statusCode() == 404) {
                return false;
            }
            throw ex; // Something else went wrong (permissions, etc.)
        }
    }


    public static HeadObjectResponse getS3ObjectMetadata(S3ClientWrapper client, S3Uri sourceUri) {
        HeadObjectRequest request = HeadObjectRequest.builder()
            .bucket(sourceUri.bucket().orElse(null))
            .key(sourceUri.key().orElse(null))
            .build();

        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                return client.getS3Client().headObject(request);
            } catch(Throwable ex) {
                LOGGER.warn(String.format("Error occurred while trying to access a file on S3: %s. Attempting to reconnect.",
                        sourceUri), ex);
                client.reconnect();
            }
        }
        // Try a last time, to throw the exception
        return client.getS3Client().headObject(request);
    }

    public static ResponseInputStream<GetObjectResponse> getS3ObjectInputStream(S3ClientWrapper client, S3Uri sourceUri) {
        GetObjectRequest request = GetObjectRequest.builder()
            .bucket(sourceUri.bucket().orElse(null))
            .key(sourceUri.key().orElse(null))
            .build();

        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                return client.getS3Client().getObject(request);
            } catch(Throwable ex) {
                LOGGER.warn(String.format("Error occurred while trying to access a file on S3: %s. Attempting to reconnect.",
                        sourceUri), ex);
                client.reconnect();
            }
        }
        // Try a last time, to throw the exception
        return client.getS3Client().getObject(request);
    }

    public S3Bucket getBucket() {
        return this.bucket;
    }

    public S3Uri getS3Uri() {
        return this.s3Uri;
    }

    public String getETag(S3ClientWrapper client) {
        if (!this.metadataFetched) {
            this.fetchMetadata(client);
        }
        return this.eTag;
    }

    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    public String getVersionId(S3ClientWrapper client) {
        if (!this.metadataFetched) {
            this.fetchMetadata(client);
        }
        return this.versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public File getLocalFile() {
        return this.localFile;
    }

    public void setLocalFile(File localFile) {
        this.localFile = localFile;
    }


    public Long getLastModified(S3ClientWrapper client) {
        if (!this.metadataFetched) {
            this.fetchMetadata(client);
        }
        return this.lastModified;
    }

    public void setLastModified(Date lastModifiedDate) {
        if (lastModifiedDate == null) {
            this.lastModified = null;
        } else {
            this.setLastModified(lastModifiedDate.getTime());
        }
    }
    public void setLastModified(Long lastModified) {
        this.lastModified = lastModified;
    }


    public Long getExpiration(S3ClientWrapper client) {
        if (!this.metadataFetched) {
            this.fetchMetadata(client);
        }
        return this.expiration;
    }

    public void setExpiration(Date expirationDate) {
        if (expirationDate == null) {
            this.expiration = null;
        } else {
            this.setExpiration(expirationDate.getTime());
        }
    }
    public void setExpiration(Long expiration) {
        this.expiration = expiration;
    }


    public Long getFileSize(S3ClientWrapper client) {
        if (!this.metadataFetched) {
            this.fetchMetadata(client);
        }
        return this.fileSize;
    }

    public void setFileSize(Long fileSize) {
        this.fileSize = fileSize;
    }

    public boolean isPublic(S3ClientWrapper client) {
        List<Grant> s3FileAcl = this.getACL(client);
        if (s3FileAcl == null) {
            return false;
        }

        return S3Utils.isPublic(s3FileAcl);
    }

    public List<Grant> getACL(S3ClientWrapper client) {
        GetObjectAclRequest aclRequest = GetObjectAclRequest.builder()
            .bucket(this.s3Uri.bucket().orElse(null))
            .key(this.s3Uri.key().orElse(null))
            .build();

        for (int i=0; i<S3_ATTEMPT; i++) {
            try {
                GetObjectAclResponse aclResponse = client.getS3Client().getObjectAcl(aclRequest);
                return aclResponse.grants();
            } catch(Throwable ex) {
                LOGGER.warn(String.format("Error occurred while accessing a file ACL on S3: %s. Attempting to reconnect.",
                        this.s3Uri), ex);
                client.reconnect();
            }
        }
        // Try a last time, to throw the exception
        GetObjectAclResponse aclResponse = client.getS3Client().getObjectAcl(aclRequest);
        return aclResponse.grants();
    }

    public JSONObject toJSON() {
        JSONObject json = new JSONObject();

        json.put("key", this.s3Uri.key().orElse(null));
        json.put("uri", this.s3Uri.uri().toString());
        json.put("filename", S3Utils.getFilename(this.s3Uri));
        json.put("directory", S3Utils.getDirectoryName(this.s3Uri));
        json.put("localFile", this.localFile);

        json.put("size", this.fileSize);
        json.put("lastModified", this.lastModified);

        json.put("eTag", this.eTag);
        json.put("versionId", this.versionId);

        return json;
    }

    @Override
    public String toString() {
        return this.toJSON().toString(4);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof S3File)) {
            return false;
        }
        return this.toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public int compareTo(S3File o) {
        return this.toString().compareTo(o.toString());
    }
}
