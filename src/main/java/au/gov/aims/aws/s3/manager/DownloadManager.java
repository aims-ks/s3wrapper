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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class DownloadManager {
	private static final Logger LOGGER = Logger.getLogger(DownloadManager.class);

	// See: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3-objects.html#download-object
	public static S3List download(S3Client client, AmazonS3URI sourceUri, File destinationFile) throws IOException {
		S3List s3List = new S3List();

		if (!client.getS3().doesBucketExistV2(sourceUri.getBucket())) {
			throw new IOException(String.format("Bucket %s doesn't exist.", sourceUri.getBucket()));
		}


		long startTime = System.currentTimeMillis();

		String filename = S3Utils.getFilename(sourceUri);
		if (filename == null || filename.isEmpty()) {
			// Download a directory
			DownloadManager.createWritableDirectory(destinationFile);

			LOGGER.fatal("Download a directory - feature not yet implemented.");
			throw new NotImplementedException();
		} else if (S3Utils.isPattern(filename)) {
			// Download all the files matching the pattern

			// The destinationFile must denote a directory (not a file)
			DownloadManager.createWritableDirectory(destinationFile);

			// Find all the matching files
			S3List filteredList = ListManager.ls(client, sourceUri);

			// Parse the JSON to find the files' URI, and download them.
			if (filteredList != null) {
				Map<String, S3File> filteredFileSet = filteredList.getFiles();
				if (filteredFileSet != null && !filteredFileSet.isEmpty()) {
					for (S3File filteredFile : filteredFileSet.values()) {
						String filteredFilename = S3Utils.getFilename(filteredFile.getS3Uri());
						s3List.putAll(DownloadManager.download(client, filteredFile.getS3Uri(), new File(destinationFile, filteredFilename)));
					}
				}
			}

		} else {
			// Download a single file

			// The destinationFile must denote a file (not a directory)
			File finalDestinationFile = destinationFile.isDirectory() ? new File(destinationFile, filename) : destinationFile;

			s3List.putFile(DownloadManager.downloadFile(client, sourceUri, finalDestinationFile));
		}

		long endTime = System.currentTimeMillis();


		s3List.setExecutionTime(endTime - startTime);

		return s3List;
	}

	private static S3File downloadFile(S3Client client, AmazonS3URI sourceUri, File destinationFile) throws IOException {
		// Download a single file
		if (destinationFile.exists()) {
			if (destinationFile.isDirectory()) {
				throw new IOException(String.format("The file %s already exists and is a directory.", destinationFile.getAbsolutePath()));
			}
			if (!destinationFile.canWrite()) {
				throw new IOException(String.format("The file %s is not writable.", destinationFile.getAbsolutePath()));
			}
		} else {
			File destinationFolder = destinationFile.getParentFile();
			DownloadManager.createWritableDirectory(destinationFolder);
		}

		S3File s3File = null;

		S3Object s3Object = null;
		S3ObjectInputStream s3FileInputStream = null;

		try {
			s3Object = client.getS3().getObject(sourceUri.getBucket(), sourceUri.getKey());
			if (s3Object != null) {
				ObjectMetadata metadata = s3Object.getObjectMetadata();
				s3File = new S3File(sourceUri, metadata);
				s3File.setLocalFile(destinationFile);

				LOGGER.debug(String.format("Downloading %s to %s", sourceUri, destinationFile));
				s3FileInputStream = s3Object.getObjectContent();
				if (s3FileInputStream != null) {
					FileUtils.copyToFile(s3FileInputStream, destinationFile);

					//FileUtils.copyToFile(s3FileInputStream, destinationFile);
					Long lastModified = s3File.getLastModified();
					if (lastModified != null) {
						boolean lastModifiedSet = destinationFile.setLastModified(lastModified);
						if (!lastModifiedSet) {
							LOGGER.warn(String.format("Could not change the last modified date of file %s, last modified timestamp: %d.",
									destinationFile.getAbsolutePath(), lastModified));
						}
					}
				} else {
					LOGGER.error(String.format("Can download the file %s, input stream is null.", sourceUri.toString()));
				}
			} else {
				LOGGER.error(String.format("Can download the file %s, file object is null.", sourceUri.toString()));
			}
		} finally {
			if (s3FileInputStream != null) {
				try {
					s3FileInputStream.close();
				} catch(Exception ex) {
					LOGGER.error("Could not close the S3 input stream.", ex);
				}
			}
			if (s3Object != null) {
				try {
					s3Object.close();
				} catch(Exception ex) {
					LOGGER.error("Could not close the S3 file.", ex);
				}
			}
		}

		return s3File;
	}

	private static void createWritableDirectory(File directory) throws IOException {
		if (directory.exists()) {
			if (!directory.isDirectory()) {
				throw new IOException(String.format("The file %s already exists and is not a directory.", directory.getAbsolutePath()));
			}
			if (!directory.canWrite()) {
				throw new IOException(String.format("The directory %s is not writable.", directory.getAbsolutePath()));
			}
		} else {
			directory.mkdirs();
			if (!directory.exists()) {
				throw new IOException(String.format("The directory %s could not be created.", directory.getAbsolutePath()));
			}
		}
	}
}
