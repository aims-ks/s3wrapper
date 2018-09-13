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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;

public class Md5 {

	// Inspired from:
	//   http://www.rgagnon.com/javadetails/java-0416.html
	public static String md5sum(File file) throws Exception {
		MessageDigest md5 = MessageDigest.getInstance("MD5");

		InputStream inputStream = null;
		DigestInputStream digestStream = null;
		try {
			inputStream = new FileInputStream(file);
			digestStream = new DigestInputStream(inputStream, md5);

			byte[] buffer = new byte[1024];
			int numRead;
			do {
				numRead = inputStream.read(buffer);
				if (numRead > 0) {
					md5.update(buffer, 0, numRead);
				}
			} while (numRead > 0);
		} finally {
			if (digestStream != null) {
				digestStream.close();
			}
			if (inputStream != null) {
				inputStream.close();
			}
		}

		byte[] bytes = md5.digest();

		// Create a HEX string from the bytes array
		StringBuilder md5sum = new StringBuilder();
		for (byte aByte : bytes) {
			md5sum.append(Integer.toString( ( aByte & 0xff ) + 0x100, 16).substring( 1 ));
		}

		return md5sum.toString();
	}
}
