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

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Uri;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class S3UtilsTest {

    @Test
    public void testS3URIFromBucketAndKey() throws URISyntaxException {
        S3Uri s3Uri;
        URI expectedUri;

        s3Uri = S3Utils.getS3URI("mybucket", "file.txt");
        expectedUri = new URI("s3", "mybucket", "/file.txt", null);
        Assert.assertEquals(expectedUri, s3Uri.uri());

        s3Uri = S3Utils.getS3URI("mybucket", "file 2.txt");
        expectedUri = new URI("s3", "mybucket", "/file 2.txt", null);
        Assert.assertEquals(expectedUri, s3Uri.uri());

        s3Uri = S3Utils.getS3URI("mybucket", "file <2>.txt");
        expectedUri = new URI("s3", "mybucket", "/file <2>.txt", null);
        Assert.assertEquals(expectedUri, s3Uri.uri());
    }

    @Test
    public void testS3URIFromUri() throws URISyntaxException {
        S3Uri s3UriFromUri;

        s3UriFromUri = S3Utils.getS3URIFromURI(new URI("s3", "mybucket", "/file.txt", null));
        Assert.assertEquals("mybucket", s3UriFromUri.bucket().orElse(null));
        Assert.assertEquals("file.txt", s3UriFromUri.key().orElse(null));

        s3UriFromUri = S3Utils.getS3URIFromURI(new URI("s3", "mybucket", "/file 2.txt", null));
        Assert.assertEquals("mybucket", s3UriFromUri.bucket().orElse(null));
        Assert.assertEquals("file 2.txt", s3UriFromUri.key().orElse(null));

        s3UriFromUri = S3Utils.getS3URIFromURI(new URI("s3", "mybucket", "/file <2>.txt", null));
        Assert.assertEquals("mybucket", s3UriFromUri.bucket().orElse(null));
        Assert.assertEquals("file <2>.txt", s3UriFromUri.key().orElse(null));
    }

    @Test
    public void testGetS3URI() {
        S3Uri s3Uri, expectedS3Uri;

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
    public void testGetPublicURLWithS3Uri() throws Exception {
        URI fileUri = new URI("s3://aims-ereefs-public-test/ncanimate/products/gbr4_v2_temp-wind-salt-current/gbr4_v2_temp-wind-salt-current_video_monthly_2011-04_torres-strait_-1.5.mp4");
        URL expectedUrl = new URL("https://aims-ereefs-public-test.s3.ap-southeast-2.amazonaws.com/ncanimate/products/gbr4_v2_temp-wind-salt-current/gbr4_v2_temp-wind-salt-current_video_monthly_2011-04_torres-strait_-1.5.mp4");

        URL actualUrl = S3Utils.getPublicURL(fileUri, S3Utils.DEFAULT_REGION);

        Assert.assertEquals("S3Utils.getPublicURL returned wrong URL", expectedUrl, actualUrl);
    }

    @Test
    public void testGetPublicURLWithFileUri() throws Exception {
        URI fileUri = new URI("file://ncanimate/products/gbr4_v2_temp-wind-salt-current/gbr4_v2_temp-wind-salt-current_video_monthly_2011-04_torres-strait_-1.5.mp4");
        URL expectedUrl = null;

        URL actualUrl = S3Utils.getPublicURL(fileUri, S3Utils.DEFAULT_REGION);

        Assert.assertEquals("S3Utils.getPublicURL returned wrong URL", expectedUrl, actualUrl);
    }

    @Test
    public void testGetFilename() {
        S3Uri s3Uri;

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
        S3Uri s3Uri, parentS3Uri;

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
        S3Uri s3Uri;

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

