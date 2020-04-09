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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Properties;

public class S3Client implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(S3Client.class);

    private static final String AWS_ACCESS_KEY_PROPERTY = "AWS_ACCESS_KEY_ID";
    private static final String AWS_SECRET_PROPERTY = "AWS_SECRET_ACCESS_KEY";
    private static final String AWS_REGION = "AWS_REGION";

    private String awsRegion;
    private AWSCredentials awsCredentials;
    private AWSCredentialsProviderChain awsCredentialsProviderChain;

    private AmazonS3 s3;

    public static S3Client parse(File credentialsPropertiesFile) throws IOException {
        if (credentialsPropertiesFile == null) {
            throw new IllegalArgumentException("File parameter can not be null");
        }

        return S3Client.parse(PropertiesLoader.load(credentialsPropertiesFile));
    }

    public static S3Client parse(Properties credentialsProperties) {
        if (credentialsProperties == null) {
            throw new IllegalArgumentException("Properties parameter can not be null");
        }

        if (!credentialsProperties.containsKey(AWS_REGION)) {
            throw new InvalidParameterException(
                String.format("The credentials file doesn't contains the AWS region property.%n" +
                 "Example: %s = ap-southeast-2", AWS_REGION)
            );
        }
        if (!credentialsProperties.containsKey(AWS_ACCESS_KEY_PROPERTY)) {
            throw new InvalidParameterException(
                String.format("The credentials file doesn't contains the access key ID property.%n" +
                 "Example: %s = AKIAIOSFODNN7EXAMPLE", AWS_ACCESS_KEY_PROPERTY)
            );
        }
        if (!credentialsProperties.containsKey(AWS_SECRET_PROPERTY)) {
            throw new InvalidParameterException(
                String.format("The credentials file doesn't contains the secret access key property.%n" +
                 "Example: %s = wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY", AWS_SECRET_PROPERTY)
            );
        }

        return new S3Client(
            credentialsProperties.getProperty(AWS_REGION),
            credentialsProperties.getProperty(AWS_ACCESS_KEY_PROPERTY),
            credentialsProperties.getProperty(AWS_SECRET_PROPERTY));
    }

    public S3Client() {
        this(null);

        // https://aws.amazon.com/blogs/developer/determining-an-applications-current-region/
        // When running on an Amazon EC2 instance, this method
        // will tell you what region your application is in
        Region region = Regions.getCurrentRegion();
        if (region == null) {
            throw new InvalidParameterException(
                "This S3Client constructor can only be used in an Amazon EC2 instance.");
        }

        this.awsRegion = region.getName();
    }

    public S3Client(String region) {
        this.awsRegion = region;

        // Inspired from:
        //   https://www.programcreek.com/java-api-examples/index.php?api=com.amazonaws.auth.EnvironmentVariableCredentialsProvider
        // See:
        //   https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
        AWSCredentialsProvider[] providers = new AWSCredentialsProvider[] {
            new EnvironmentVariableCredentialsProvider(),
            new EC2ContainerCredentialsProviderWrapper(),
            InstanceProfileCredentialsProvider.getInstance(),
            new SystemPropertiesCredentialsProvider(),
            new ProfileCredentialsProvider()
        };

        this.awsCredentialsProviderChain = new AWSCredentialsProviderChain(providers);
    }

    public S3Client(String region, String accessKeyId, String secretAccessKey) {
        this.awsRegion = region;
        this.awsCredentials =
                new BasicAWSCredentials(accessKeyId, secretAccessKey);
    }

    public void reconnect() {
        this.shutdown();

        // Creates a "AmazonS3Client"
        if (this.awsCredentials != null) {
            this.s3 = AmazonS3ClientBuilder.standard()
                    .withRegion(this.awsRegion)
                    .withCredentials(new AWSStaticCredentialsProvider(this.awsCredentials))
                    .build();
        } else if (this.awsCredentialsProviderChain != null) {
            this.s3 = AmazonS3ClientBuilder.standard()
                    .withRegion(this.awsRegion)
                    .withCredentials(new AWSCredentialsProviderChain(this.awsCredentialsProviderChain))
                    .build();
        }
    }

    public AmazonS3 getS3() {
        if (this.s3 == null) {
            this.reconnect();
        }
        return this.s3;
    }

    @Override
    public void close() throws IOException {
        this.shutdown();
    }

    private void shutdown() {
        if (this.s3 != null) {
            try {
                this.s3.shutdown();
            } catch(Throwable ex) {
                LOGGER.warn("Error occurred while shutting down a S3 client", ex);
            }
        }
        this.s3 = null;
    }
}
