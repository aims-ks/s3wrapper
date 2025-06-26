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

import au.gov.aims.aws.s3.PropertiesLoader;
import au.gov.aims.aws.s3.S3Utils;
import org.apache.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Properties;

public class S3ClientWrapper implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(S3ClientWrapper.class);

    private static final String AWS_ACCESS_KEY_PROPERTY = "AWS_ACCESS_KEY_ID";
    private static final String AWS_SECRET_PROPERTY = "AWS_SECRET_ACCESS_KEY";
    private static final String AWS_REGION = "AWS_REGION";

    private Region awsRegion;
    private StaticCredentialsProvider awsCredentials;
    private DefaultCredentialsProvider defaultCredentialsProvider;

    private S3Client s3Client;
    private S3AsyncClient s3AsyncClient;

    public static S3ClientWrapper parse(File credentialsPropertiesFile) throws IOException {
        if (credentialsPropertiesFile == null) {
            throw new IllegalArgumentException("File parameter can not be null");
        }

        return S3ClientWrapper.parse(PropertiesLoader.load(credentialsPropertiesFile));
    }

    public static S3ClientWrapper parse(Properties credentialsProperties) {
        if (credentialsProperties == null) {
            throw new IllegalArgumentException("Properties parameter can not be null");
        }

        if (!credentialsProperties.containsKey(AWS_REGION)) {
            throw new InvalidParameterException(
                String.format("The credentials file doesn't contain the AWS region property.%n" +
                 "Example: %s = ap-southeast-2", AWS_REGION)
            );
        }
        if (!credentialsProperties.containsKey(AWS_ACCESS_KEY_PROPERTY)) {
            throw new InvalidParameterException(
                String.format("The credentials file doesn't contain the access key ID property.%n" +
                 "Example: %s = AKIAIOSFODNN7EXAMPLE", AWS_ACCESS_KEY_PROPERTY)
            );
        }
        if (!credentialsProperties.containsKey(AWS_SECRET_PROPERTY)) {
            throw new InvalidParameterException(
                String.format("The credentials file doesn't contain the secret access key property.%n" +
                 "Example: %s = wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY", AWS_SECRET_PROPERTY)
            );
        }

        return new S3ClientWrapper(
            credentialsProperties.getProperty(AWS_REGION),
            credentialsProperties.getProperty(AWS_ACCESS_KEY_PROPERTY),
            credentialsProperties.getProperty(AWS_SECRET_PROPERTY));
    }

    public S3ClientWrapper() {
        this(null);

        // https://aws.amazon.com/blogs/developer/determining-an-applications-current-region/
        // When running on an Amazon EC2 instance, this method
        // will tell you what region your application is in
        Region region = S3Utils.getCurrentRegion();
        if (region == null) {
            throw new InvalidParameterException(
                "This S3Client constructor can only be used in an Amazon EC2 instance.");
        }

        this.awsRegion = region;
    }

    public S3ClientWrapper(String regionStr) {
        Region region = Region.of(regionStr);
        this.awsRegion = region == null ? S3Utils.DEFAULT_REGION : region;
        this.defaultCredentialsProvider = DefaultCredentialsProvider.builder().build();
    }

    public S3ClientWrapper(String regionStr, String accessKeyId, String secretAccessKey) {
        Region region = Region.of(regionStr);
        this.awsRegion = region == null ? S3Utils.DEFAULT_REGION : region;

        this.awsCredentials = StaticCredentialsProvider.create(AwsBasicCredentials.create(
            accessKeyId,
            secretAccessKey
        ));
    }

    public void reconnect() {
        this.shutdown();

        // Creates a S3Client and a S3AsyncClient
        if (this.awsCredentials != null) {
            this.s3Client = S3Client.builder()
                    .region(this.awsRegion)
                    .credentialsProvider(this.awsCredentials)
                    .build();

            this.s3AsyncClient = S3AsyncClient.builder()
                    .region(this.awsRegion)
                    .credentialsProvider(this.awsCredentials)
                    .build();
        } else if (this.defaultCredentialsProvider != null) {
            this.s3Client = S3Client.builder()
                    .region(this.awsRegion)
                    .credentialsProvider(this.defaultCredentialsProvider)
                    .build();

            this.s3AsyncClient = S3AsyncClient.builder()
                    .region(this.awsRegion)
                    .credentialsProvider(this.defaultCredentialsProvider)
                    .build();
        }
    }

    public S3Client getS3Client() {
        if (this.s3Client == null) {
            this.reconnect();
        }
        return this.s3Client;
    }

    public S3AsyncClient getS3AsyncClient() {
        if (this.s3AsyncClient == null) {
            this.reconnect();
        }
        return this.s3AsyncClient;
    }

    @Override
    public void close() throws IOException {
        this.shutdown();
    }

    private void shutdown() {
        if (this.s3Client != null) {
            try {
                this.s3Client.close();
            } catch(Throwable ex) {
                LOGGER.warn("Error occurred while shutting down a S3 client", ex);
            }
        }
        this.s3Client = null;

        if (this.s3AsyncClient != null) {
            try {
                this.s3AsyncClient.close();
            } catch(Throwable ex) {
                LOGGER.warn("Error occurred while shutting down a S3 client", ex);
            }
        }
        this.s3AsyncClient = null;
    }
}
