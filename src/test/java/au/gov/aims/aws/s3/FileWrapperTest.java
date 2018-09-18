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

import au.gov.aims.aws.s3.entity.S3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;

public class FileWrapperTest extends S3TestBase {
	private static final Logger LOGGER = Logger.getLogger(FileWrapperTest.class);

	@Test
	public void testListFilesWithoutS3() throws Exception {
		URL ioFileUrl = FileWrapperTest.class.getClassLoader().getResource("bucket_files");
		File ioFile = new File(ioFileUrl.toURI());
		FileWrapper rootFileWrapper = new FileWrapper(null, ioFile);

		FileWrapper[] fileWrapperList = rootFileWrapper.listFiles(null);
		Assert.assertNotNull("The file list is null", fileWrapperList);
		Assert.assertEquals("Wrong number of files in the bucket.", 3, fileWrapperList.length);

		for (FileWrapper fileWrapper : fileWrapperList) {
			String filename = fileWrapper.getFile().getName();

			if ("bin".equals(filename)) {
				Assert.assertEquals("The file was not as expected", new File(ioFile, "bin"), fileWrapper.getFile());
				// List files in "bin"
				FileWrapper[] binFileWrapperList = fileWrapper.listFiles(null);
				Assert.assertNotNull("The 'bin' file list is null", binFileWrapperList);
				Assert.assertEquals("Wrong number of files in the 'bin'.", 3, binFileWrapperList.length);

				// Check files in "bin"
				for (FileWrapper binFileWrapper : binFileWrapperList) {
					String binFilename = binFileWrapper.getFile().getName();

					if ("random_100.bin".equals(binFilename)) {
						Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/random_100.bin"), binFileWrapper.getFile());

					} else if ("random_1024.bin".equals(binFilename)) {
						Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/random_1024.bin"), binFileWrapper.getFile());

					} else if ("zero_100.bin".equals(binFilename)) {
						Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/zero_100.bin"), binFileWrapper.getFile());
						Assert.assertTrue(String.format("The bin file %s does not exist.", binFileWrapper.getFile()), binFileWrapper.getFile().exists());

					} else {
						Assert.fail(String.format("Unexpected file key in 'bin': '%s'", binFilename));
					}
				}

			} else if ("img".equals(filename)) {
				Assert.assertEquals("The file was not as expected", new File(ioFile, "img"), fileWrapper.getFile());

			} else if ("root.txt".equals(filename)) {
				Assert.assertEquals("The file was not as expected", new File(ioFile, "root.txt"), fileWrapper.getFile());
				Assert.assertTrue(String.format("The file %s does not exist.", fileWrapper.getFile()), fileWrapper.getFile().exists());

			} else {
				Assert.fail(String.format("Unexpected filename: '%s'", filename));
			}
		}
	}

	/**
	 * Upload a file to S3, then download it to see if it has changed.
	 * NOTE: The resource file "aws-credentials.properties" must be set before running this test.
	 * @throws Exception If something goes wrong...
	 */
	@Test
	public void testListFiles() throws Exception {
		AmazonS3URI s3Uri = S3Utils.getS3URI(S3TestBase.S3_BUCKET_ID);
		File ioFile = new File("/tmp/s3wrapper/FileWrapper");
		FileWrapper rootFileWrapper = new FileWrapper(s3Uri, ioFile);

		// Delete the folder on disk if it already exists
		FileUtils.deleteDirectory(ioFile);

		try (S3Client client = super.openS3Client()) {
			super.setupBucket(client);

			FileWrapper[] fileWrapperList = rootFileWrapper.listFiles(client);
			Assert.assertNotNull("The file list is null", fileWrapperList);
			Assert.assertEquals("Wrong number of files in the bucket.", 3, fileWrapperList.length);

			for (FileWrapper fileWrapper : fileWrapperList) {
				String filename = fileWrapper.getFile().getName();

				if ("bin".equals(filename)) {
					Assert.assertEquals("The file was not as expected", new File(ioFile, "bin"), fileWrapper.getFile());
					// List files in "bin"
					FileWrapper[] binFileWrapperList = fileWrapper.listFiles(client);
					Assert.assertNotNull("The 'bin' file list is null", binFileWrapperList);
					Assert.assertEquals("Wrong number of files in the 'bin'.", 3, binFileWrapperList.length);

					// Check files in "bin"
					for (FileWrapper binFileWrapper : binFileWrapperList) {
						String binFilename = binFileWrapper.getFile().getName();

						if ("random_100.bin".equals(binFilename)) {
							Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/random_100.bin"), binFileWrapper.getFile());

						} else if ("random_1024.bin".equals(binFilename)) {
							Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/random_1024.bin"), binFileWrapper.getFile());

						} else if ("zero_100.bin".equals(binFilename)) {
							Assert.assertEquals("The bin file was not as expected", new File(ioFile, "bin/zero_100.bin"), binFileWrapper.getFile());

							// Try to download the "bin/zero_100.bin" file
							Assert.assertFalse(String.format("The bin file %s already exists.", binFileWrapper.getFile()), binFileWrapper.getFile().exists());
							File downloadedFile = binFileWrapper.downloadFile(client);
							Assert.assertEquals("The downloaded bin file is different from expectation.", binFileWrapper.getFile(), downloadedFile);
							Assert.assertTrue(String.format("The bin file %s was not downloaded.", binFileWrapper.getFile()), binFileWrapper.getFile().exists());

						} else {
							Assert.fail(String.format("Unexpected file key in 'bin': '%s'", binFilename));
						}
					}

				} else if ("img".equals(filename)) {
					Assert.assertEquals("The file was not as expected", new File(ioFile, "img"), fileWrapper.getFile());

				} else if ("root.txt".equals(filename)) {
					Assert.assertEquals("The file was not as expected", new File(ioFile, "root.txt"), fileWrapper.getFile());

					// Try to download the "root.txt" file
					Assert.assertFalse(String.format("The file %s already exists.", fileWrapper.getFile()), fileWrapper.getFile().exists());
					File downloadedFile = fileWrapper.downloadFile(client);
					Assert.assertEquals("The downloaded file is different from expectation.", fileWrapper.getFile(), downloadedFile);
					Assert.assertTrue(String.format("The file %s was not downloaded.", fileWrapper.getFile()), fileWrapper.getFile().exists());

				} else {
					Assert.fail(String.format("Unexpected filename: '%s'", filename));
				}
			}
		}

		// Cleanup at the end of the test
		FileUtils.deleteDirectory(ioFile);
	}
}