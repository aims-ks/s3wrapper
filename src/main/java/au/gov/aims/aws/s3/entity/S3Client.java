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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Properties;

public class S3Client implements Closeable {
	private static final String AWS_ACCESS_KEY_PROPERTY = "AWS_ACCESS_KEY_ID";
	private static final String AWS_SECRET_PROPERTY = "AWS_SECRET_ACCESS_KEY";

	private AmazonS3 s3;

	public static S3Client parse(File credentialsPropertiesFile) throws Exception {
		if (credentialsPropertiesFile == null) {
			return new S3Client();
		}

		return S3Client.parse(PropertiesLoader.load(credentialsPropertiesFile));
	}

	public static S3Client parse(Properties credentialsProperties) throws Exception {
		if (credentialsProperties == null) {
			return new S3Client();
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
			credentialsProperties.getProperty(AWS_ACCESS_KEY_PROPERTY),
			credentialsProperties.getProperty(AWS_SECRET_PROPERTY));
	}

	public S3Client() {
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

		// Creates a "AmazonS3Client"
		this.s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSCredentialsProviderChain(providers))
				.build();
	}

	public S3Client(String accessKeyId, String secretAccessKey) {
		AWSCredentials awsCredentials =
				new BasicAWSCredentials(accessKeyId, secretAccessKey);

		// Creates a "AmazonS3Client"
		this.s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
				.build();
	}

	public S3Client(AmazonS3 s3) {
		this.s3 = s3;
	}

	public AmazonS3 getS3() {
		return this.s3;
	}

	@Override
	public void close() throws IOException {
		this.s3.shutdown();
	}
}
