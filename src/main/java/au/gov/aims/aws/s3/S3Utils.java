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
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;

import java.util.List;
import java.util.regex.Pattern;

public class S3Utils {

	public static String getFilename(AmazonS3URI s3Uri) {
		if (s3Uri == null) {
			return null;
		}

		String key = s3Uri.getKey();
		if (key == null || key.isEmpty() || key.endsWith("/")) {
			return null;
		}

		int lastSlashIdx = key.lastIndexOf('/');

		return lastSlashIdx < 0 ? key : key.substring(lastSlashIdx + 1);
	}

	public static String getDirectoryName(AmazonS3URI s3Uri) {
		if (s3Uri == null) {
			return null;
		}

		String key = s3Uri.getKey();
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

	public static AmazonS3URI getParentUri(AmazonS3URI s3Uri) {
		if (s3Uri == null) {
			return null;
		}

		String key = s3Uri.getKey();
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

		if (!parentKey.startsWith("/")) {
			// Remove the trailing slash
			parentKey = '/' + parentKey;
		}

		return new AmazonS3URI("s3://" + s3Uri.getBucket() + parentKey);
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

	public static boolean isPublic(AccessControlList acl) {
		List<Grant> grants = acl.getGrantsAsList();
		for (Grant grant : grants) {
			// Find the grant for all users (the Public grant)
			if (GroupGrantee.AllUsers.equals(grant.getGrantee())) {

				// Check if it's readable
				Permission grantPermission = grant.getPermission();
				boolean canRead = grantPermission == Permission.Read ||
					grantPermission == Permission.FullControl;

				if (canRead) {
					return true;
				}
			}
		}

		return false;
	}
}
