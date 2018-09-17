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

import com.amazonaws.services.s3.AmazonS3URI;
import org.junit.Assert;
import org.junit.Test;

public class S3UtilsTest {

	@Test
	public void testGetS3URI() {
		AmazonS3URI s3Uri, expectedS3Uri;

		s3Uri = S3Utils.getS3URI("mybucket");
		expectedS3Uri = S3Utils.getS3URI("mybucket", "/");
		Assert.assertEquals(expectedS3Uri, s3Uri);

		s3Uri = S3Utils.getS3URI("mybucket", null);
		expectedS3Uri = S3Utils.getS3URI("mybucket", "/");
		Assert.assertEquals(expectedS3Uri, s3Uri);

		s3Uri = S3Utils.getS3URI("mybucket", "");
		expectedS3Uri = S3Utils.getS3URI("mybucket", "/");
		Assert.assertEquals(expectedS3Uri, s3Uri);

		s3Uri = S3Utils.getS3URI("mybucket", "/");
		expectedS3Uri = S3Utils.getS3URI("mybucket", "/");
		Assert.assertEquals(expectedS3Uri, s3Uri);

		s3Uri = S3Utils.getS3URI("mybucket", "folder", "file");
		expectedS3Uri = S3Utils.getS3URI("mybucket", "folder/file");
		Assert.assertEquals(expectedS3Uri, s3Uri);

		s3Uri = S3Utils.getS3URI("mybucket", "folder/file");
		expectedS3Uri = S3Utils.getS3URI("mybucket", "folder/file");
		Assert.assertEquals(expectedS3Uri, s3Uri);

		s3Uri = S3Utils.getS3URI("mybucket", "/folder/", "/file");
		expectedS3Uri = S3Utils.getS3URI("mybucket", "folder/file");
		Assert.assertEquals(expectedS3Uri, s3Uri);

		s3Uri = S3Utils.getS3URI("mybucket", "///folder///", "///file");
		expectedS3Uri = S3Utils.getS3URI("mybucket", "folder/file");
		Assert.assertEquals(expectedS3Uri, s3Uri);

		s3Uri = S3Utils.getS3URI("mybucket", "///folder//subfolder/////file");
		expectedS3Uri = S3Utils.getS3URI("mybucket", "folder/subfolder/file");
		Assert.assertEquals(expectedS3Uri, s3Uri);
	}

	@Test
	public void testGetFilename() {
		AmazonS3URI s3Uri;

		s3Uri = S3Utils.getS3URI("mybucket", "/folder/file.txt");
		Assert.assertEquals("file.txt", S3Utils.getFilename(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/file.txt");
		Assert.assertEquals("file.txt", S3Utils.getFilename(s3Uri));

		// Do not end with a "/", therefor it's a file
		s3Uri = S3Utils.getS3URI("mybucket", "/file");
		Assert.assertEquals("file", S3Utils.getFilename(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/folder/");
		Assert.assertNull(S3Utils.getFilename(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/");
		Assert.assertNull(S3Utils.getFilename(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket");
		Assert.assertNull(S3Utils.getFilename(s3Uri));
	}

	@Test
	public void testGetParentUri() {
		AmazonS3URI s3Uri, parentS3Uri;

		s3Uri = S3Utils.getS3URI("mybucket", "/file.txt");
		parentS3Uri = S3Utils.getS3URI("mybucket", "/");
		Assert.assertEquals(parentS3Uri, S3Utils.getParentUri(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/folder/file.txt");
		parentS3Uri = S3Utils.getS3URI("mybucket", "/folder/");
		Assert.assertEquals(parentS3Uri, S3Utils.getParentUri(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/folder/subfolder/file");
		parentS3Uri =  S3Utils.getS3URI("mybucket", "/folder/subfolder/");
		Assert.assertEquals(parentS3Uri, S3Utils.getParentUri(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/folder/subfolder/");
		parentS3Uri = S3Utils.getS3URI("mybucket", "/folder/");
		Assert.assertEquals(parentS3Uri, S3Utils.getParentUri(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/folder/");
		parentS3Uri = S3Utils.getS3URI("mybucket", "/");
		Assert.assertEquals(parentS3Uri, S3Utils.getParentUri(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/");
		parentS3Uri = S3Utils.getS3URI("mybucket", "/");
		Assert.assertEquals(parentS3Uri, S3Utils.getParentUri(s3Uri));

		// Chain
		s3Uri = S3Utils.getS3URI("mybucket", "/folder/subfolder/file");
		parentS3Uri = S3Utils.getS3URI("mybucket", "/folder/");
		Assert.assertEquals(parentS3Uri, S3Utils.getParentUri(S3Utils.getParentUri(s3Uri)));
	}

	@Test
	public void testGetDirectoryName() {
		AmazonS3URI s3Uri;

		s3Uri = S3Utils.getS3URI("mybucket");
		Assert.assertNull(S3Utils.getDirectoryName(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/");
		Assert.assertNull(S3Utils.getDirectoryName(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/file.txt");
		Assert.assertNull(S3Utils.getDirectoryName(s3Uri));


		s3Uri = S3Utils.getS3URI("mybucket", "/folder/");
		Assert.assertEquals("folder", S3Utils.getDirectoryName(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/folder/file.txt");
		Assert.assertEquals("folder", S3Utils.getDirectoryName(s3Uri));


		s3Uri = S3Utils.getS3URI("mybucket", "/folder/subfolder/");
		Assert.assertEquals("subfolder", S3Utils.getDirectoryName(s3Uri));

		s3Uri = S3Utils.getS3URI("mybucket", "/folder/subfolder/file");
		Assert.assertEquals("subfolder", S3Utils.getDirectoryName(s3Uri));
	}
}

