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

import org.apache.log4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.Type;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.regex.Pattern;

public class S3Utils {
    public static final Region DEFAULT_REGION = Region.AP_SOUTHEAST_2;

    protected static final String S3_SCHEME = "s3";
    protected static final String S3_PROTOCOL = S3_SCHEME + "://";

    private static final Logger LOGGER = Logger.getLogger(S3Utils.class);

    // Used when parsing URLs from config files
    public static boolean isS3URI(String uri) {
        if (uri == null) {
            return false;
        }

        // Check if it starts with the s3 protocol
        if (!uri.startsWith(S3Utils.S3_PROTOCOL)) {
            return false;
        }

        // Check if there is something after the s3 protocol
        // (i.e. if there is a bucket id)
        if (uri.length() <= S3Utils.S3_PROTOCOL.length()) {
            return false;
        }

        // Check if the bucket name is empty
        if (uri.startsWith(S3Utils.S3_PROTOCOL + '/')) {
            return false;
        }

        return true;
    }

    public static S3Uri getS3URI(String bucketId) {
        return S3Utils.getS3URI(bucketId, null);
    }

    public static S3Uri getS3URI(String bucket, String folder, String filename) {
        if (filename == null || filename.isEmpty()) {
            return S3Utils.getS3URI(bucket, folder);
        }

        if (folder == null || folder.isEmpty()) {
            folder = "";
        } else if (!folder.endsWith("/")) {
            folder += '/';
        }

        String key = folder + filename;

        return getS3URI(bucket, key);
    }

    public static S3Uri getS3URI(String bucket, String key) {
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException("Bucket ID must not be null.");
        }
        if (bucket.contains("/")) {
            throw new IllegalArgumentException("Invalid Bucket ID: " + bucket);
        }

        return S3Utils.buildS3URI(bucket, key, null);
    }

    public static S3Uri getS3URIFromURI(URI fileUri) {
        return S3Utils.buildS3URI(null, null, fileUri);
    }

    private static S3Uri buildS3URI(String bucket, String key, URI s3FileUri) {
        boolean validBucket = bucket != null && !bucket.isEmpty();
        boolean validUri = s3FileUri != null && S3Utils.S3_SCHEME.equals(s3FileUri.getScheme());

        if (!validBucket && !validUri) {
            return null;
        }

        if (key == null || key.isEmpty()) {
            key = "";
        } else {
            key = key.replaceAll("/{2,}", "/");
        }

        if (key.startsWith("/")) {
            key = key.substring(1);
        }

        if (!validUri) {
            // Craft a URI from the bucket and key
            try {
                s3FileUri = new URI("s3", bucket, "/" + key, null);
            } catch (URISyntaxException ex) {
                return null;
            }
        }

        if (!validBucket) {
            // Extract the bucket and key from the URI
            S3Client s3client = S3Client.create(); // Dummy client
            S3Utilities utilities = s3client.utilities();

            return utilities.parseUri(s3FileUri);
        }

        return S3Uri.builder()
            .bucket(bucket)
            .key(key.isEmpty() ? null : key)
            .uri(s3FileUri)
            .build();
    }

    /**
     * Build a public URL from a S3 URI.
     * See: https://docs.aws.amazon.com/fr_fr/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro
     * @param fileUri
     * @return
     */
    public static URL getPublicURL(URI fileUri, Region region) {
        S3Uri s3Uri = S3Utils.getS3URIFromURI(fileUri);
        if (s3Uri != null) {
            String bucket = s3Uri.bucket().orElseThrow(() -> new IllegalArgumentException("Missing bucket"));
            String key = s3Uri.key().orElse("");

            try {
                URI uri = new URI(
                    "https",
                    bucket + ".s3." + region.id() + ".amazonaws.com",
                    "/" + key,
                    null // query string
                );
                return uri.toURL();
            } catch(Exception ex) {
                LOGGER.error(String.format("Could not create a public S3 URL from S3 URI: %s", fileUri), ex);
                return null;
            }
        }

        return null;
    }

    public static String getFilename(S3Uri s3Uri) {
        if (s3Uri == null) {
            return null;
        }

        String key = s3Uri.key().orElse(null);
        if (key == null || key.isEmpty() || key.endsWith("/")) {
            return null;
        }

        int lastSlashIdx = key.lastIndexOf('/');

        return lastSlashIdx < 0 ? key : key.substring(lastSlashIdx + 1);
    }

    public static String getDirectoryName(S3Uri s3Uri) {
        if (s3Uri == null) {
            return null;
        }

        String key = s3Uri.key().orElse(null);
        if (key == null || key.isEmpty()) {
            return null;
        }

        if (key.endsWith("/")) {
            // Remove the trailing slash
            key = key.substring(0, key.length() - 1);
        } else {
            // The URI denote a file. Find the parent folder
            int lastSlashIdx = key.lastIndexOf('/');

            if (lastSlashIdx < 0) {
                return null;
            } else {
                key = key.substring(0, lastSlashIdx);
            }
        }


        int lastSlashIdx = key.lastIndexOf('/');

        return lastSlashIdx < 0 ? key : key.substring(lastSlashIdx + 1);
    }

    public static S3Uri getParentUri(S3Uri s3Uri) {
        if (s3Uri == null) {
            return null;
        }

        String key = s3Uri.key().orElse(null);
        if (key == null || key.isEmpty()) {
            return s3Uri;
        }

        if (key.endsWith("/")) {
            // Remove the trailing slash
            key = key.substring(0, key.length() - 2);
        }

        int lastSlashIdx = key.lastIndexOf('/');

        String parentKey;
        if (lastSlashIdx < 0) {
            parentKey = "";
        } else {
            parentKey = key.substring(0, lastSlashIdx + 1);
        }

        return S3Utils.getS3URI(s3Uri.bucket().orElse(null), parentKey);
    }

    public static boolean isPattern(String str) {
        return str != null && str.contains("*");
    }

    public static Pattern toPattern(String str) {
        if (str == null) {
            return null;
        }

        str = str.trim();
        if (str.isEmpty() || !S3Utils.isPattern(str)) {
            return null;
        }

        // Replace multiple consecutive occurrences of "*" with a single "*".
        String rawPattern = str.replaceAll("\\*{2,}", "*");

        StringBuilder patternSb = new StringBuilder();
        patternSb.append('^');

        int lastStarIndex = -1,
                starIndex = rawPattern.indexOf("*");
        while (starIndex >= 0) {
            if (starIndex == 0) {
                patternSb.append(".*");
            } else {
                patternSb.append(Pattern.quote(rawPattern.substring(lastStarIndex+1, starIndex)));
                patternSb.append(".*");
            }

            lastStarIndex = starIndex;
            starIndex = rawPattern.indexOf("*", starIndex + 1);
        }
        if (lastStarIndex < rawPattern.length() - 1) {
            patternSb.append(Pattern.quote(rawPattern.substring(lastStarIndex+1)));
        }

        patternSb.append('$');

        return Pattern.compile(patternSb.toString());
    }

    public static boolean isPublic(List<Grant> acl) {
        for (Grant grant : acl) {
            Grantee grantee = grant.grantee();
            if (grantee != null && grantee.type() == Type.GROUP &&
                "http://acs.amazonaws.com/groups/global/AllUsers".equals(grantee.uri())) {
                Permission permission = grant.permission();
                if (permission == Permission.READ || permission == Permission.FULL_CONTROL) {
                    return true; // Public read access via ACL
                }
            }
        }

        return false;
    }

    public static Region getCurrentRegion() {
        return new DefaultAwsRegionProviderChain().getRegion();
    }
}
