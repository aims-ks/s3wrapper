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
import au.gov.aims.aws.s3.entity.S3File;
import au.gov.aims.aws.s3.entity.S3List;
import au.gov.aims.aws.s3.manager.DownloadManager;
import au.gov.aims.aws.s3.manager.ListManager;
import au.gov.aims.aws.s3.manager.UploadManager;
import com.amazonaws.services.s3.AmazonS3URI;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class can be used to handle both io.File and S3File.
 */
public class FileWrapper implements Comparable<FileWrapper> {
	private AmazonS3URI s3URI;
	private File ioFile;
	private boolean downloaded = false;

	public FileWrapper(AmazonS3URI s3URI, File ioFile) {
		this.ioFile = ioFile;
		this.s3URI = s3URI;
	}

	public FileWrapper(FileWrapper parent, String pathname) {
		this.ioFile = parent.ioFile == null ? null :
			new File(parent.ioFile, pathname);

		this.s3URI = parent.s3URI == null ? null :
			S3Utils.getS3URI(parent.s3URI.getBucket(), parent.s3URI.getKey() + "/" + pathname);
	}

	public FileWrapper getParent() {
		return new FileWrapper(
			S3Utils.getParentUri(this.s3URI),
			this.ioFile == null ? null : this.ioFile.getParentFile());
	}

	public boolean isDirectory() {
		if (this.s3URI != null) {
			return this.s3URI.getKey().endsWith("/");
		}

		if (this.ioFile != null) {
			return this.ioFile.isDirectory();
		}

		return false;
	}

	public boolean exists(S3Client client) {
		if (this.s3URI != null) {
			return client.getS3().doesObjectExist(this.s3URI.getBucket(), this.s3URI.getKey());
		}

		if (this.ioFile != null) {
			return this.ioFile.exists();
		}

		return false;
	}

	public String getFilename() {
		if (this.s3URI != null) {
			return S3Utils.getFilename(this.s3URI);
		}

		if (this.ioFile != null) {
			return this.ioFile.getName();
		}

		return null;
	}

	public File getFile() {
		return this.ioFile;
	}

	public AmazonS3URI getS3URI() {
		return this.s3URI;
	}


	public File downloadFile(S3Client client) throws IOException {
		return this.downloadFile(client, false);
	}

	public File downloadFile(S3Client client, boolean forceDownload) throws IOException {
		if (client != null && this.s3URI != null && this.ioFile != null) {
			boolean downloadedNeeded = false;

			if (forceDownload) {
				downloadedNeeded = true;
			} else {
				// If there is no filename, the S3URI denote a folder
				//   (we won't download a whole folder)
				String filename = S3Utils.getFilename(this.s3URI);
				if (filename != null && !filename.isEmpty()) {
					if (!this.ioFile.exists()) {
						downloadedNeeded = true;
					} else {
						S3List s3List = ListManager.ls(client, this.s3URI);
						S3File s3File = s3List.getFiles().get(this.s3URI.getKey());

						if (this.ioFile.lastModified() < s3File.getLastModified()) {
							downloadedNeeded = true;
						}
					}
				}
			}

			if (downloadedNeeded) {
				this.forceDownloadFile(client);
			}
		}

		return this.ioFile;
	}

	private void forceDownloadFile(S3Client client) throws IOException {
		if (client != null && this.s3URI != null && this.ioFile != null) {
			DownloadManager.download(client, this.s3URI, this.ioFile);
			this.downloaded = true;
		}
	}

	/**
	 * Delete the file if it was downloaded from S3.
	 * @return True if the file was deleted from disk. False otherwise.
	 */
	public boolean cleanup() {
		if (this.downloaded && this.ioFile != null && this.ioFile.exists()) {
			return this.ioFile.delete();
		}

		return false;
	}

	public void uploadFile(S3Client client) throws IOException, InterruptedException {
		if (client != null && this.s3URI != null && this.ioFile != null) {
			UploadManager.upload(client, this.ioFile, this.s3URI);
		}
	}


	// Similar to io.File.listFiles()
	public List<FileWrapper> listFiles(S3Client client) {
		return this.listFiles(client, null, null, false);
	}
	public List<FileWrapper> listFiles(S3Client client, boolean recursive) {
		return this.listFiles(client, null, null, recursive);
	}

	// Similar to io.File.listFiles(FilenameFilter)
	public List<FileWrapper> listFiles(S3Client client, FilenameFilter filenameFilter) {
		return this.listFiles(client, filenameFilter, null, false);
	}
	public List<FileWrapper> listFiles(S3Client client, FilenameFilter filenameFilter, boolean recursive) {
		return this.listFiles(client, filenameFilter, null, recursive);
	}

	// Similar to io.File.listFiles(FileFilter)
	public List<FileWrapper> listFiles(S3Client client, FileFilter fileFilter) {
		return this.listFiles(client, null, fileFilter, false);
	}
	public List<FileWrapper> listFiles(S3Client client, FileFilter fileFilter, boolean recursive) {
		return this.listFiles(client, null, fileFilter, recursive);
	}

	// Similar to FileUtils.listFiles(File, String[], boolean)
	public List<FileWrapper> listFiles(S3Client client, final String[] extensions, boolean recursive) {
		return this.listFiles(client, new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if (extensions == null) {
					return true;
				}
				for (String extension : extensions) {
					if (name.endsWith("." + extension)) {
						return true;
					}
				}
				return false;
			}
		}, null, recursive);
	}

	private List<FileWrapper> listFiles(S3Client client, FilenameFilter filenameFilter, FileFilter fileFilter, boolean recursive) {
		List<FileWrapper> fileWrappers = null;

		if (client != null && this.s3URI != null) {
			S3List s3List;
			if (filenameFilter != null) {
				s3List = ListManager.ls(client, this.s3URI, filenameFilter, recursive);
			} else if (fileFilter != null) {
				s3List = ListManager.ls(client, this.s3URI, fileFilter, recursive);
			} else {
				s3List = ListManager.ls(client, this.s3URI, recursive);
			}

			if (s3List != null) {
				fileWrappers = this.toFileWrapperList(s3List);
			}

		} else if (this.ioFile != null) {
			File[] files;
			if (filenameFilter != null) {
				files = this.ioFile.listFiles(filenameFilter);
			} else if (fileFilter != null) {
				files = this.ioFile.listFiles(fileFilter);
			} else {
				files = this.ioFile.listFiles();
			}

			if (files != null && files.length > 0) {
				fileWrappers = new ArrayList<FileWrapper>(files.length);

				for (File file : files) {
					fileWrappers.add(new FileWrapper(null, file));
				}


				if (recursive) {
					// List all directories
					// NOTE: This is not optimal but that seems
					//   to be the best way to do it in java:
					//   - https://stackoverflow.com/questions/5125242/java-list-only-subdirectories-from-a-directory-not-files
					//   - https://stackoverflow.com/questions/1034977/how-to-retrieve-a-list-of-directories-quickly-in-java
					File[] dirs = this.ioFile.listFiles(new FilenameFilter() {
						@Override
						public boolean accept(File parent, String filename) {
							return new File(parent, filename).isDirectory();
						}
					});

					if (dirs != null) {
						for (File dir : dirs) {
							FileWrapper dirFileWrapper = new FileWrapper(null, dir);
							fileWrappers.addAll(
								dirFileWrapper.listFiles(client, filenameFilter, fileFilter, recursive));
						}
					}
				}
			}
		}

		return fileWrappers;
	}


	private List<FileWrapper> toFileWrapperList(S3List s3List) {
		List<FileWrapper> fileWrapperList = new ArrayList<FileWrapper>();

		if (s3List != null) {
			String thisKey = this.s3URI == null ? "" : this.s3URI.getKey();
			if (thisKey == null) {
				thisKey = "";
			}

			Map<String, S3File> dirs = s3List.getDirs();
			for (S3File s3File : dirs.values()) {
				AmazonS3URI dirUri = s3File.getS3Uri();
				String dirKey = dirUri.getKey();
				String dirKeySuffix = dirKey.substring(thisKey.length());

				fileWrapperList.add(new FileWrapper(dirUri, new File(this.ioFile, dirKeySuffix)));
			}

			Map<String, S3File> files = s3List.getFiles();
			for (S3File s3File : files.values()) {
				AmazonS3URI fileUri = s3File.getS3Uri();
				String fileKey = fileUri.getKey();
				String fileKeySuffix = fileKey.substring(thisKey.length());

				fileWrapperList.add(new FileWrapper(fileUri, new File(this.ioFile, fileKeySuffix)));
			}
		}

		return fileWrapperList;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		if (this.s3URI != null) {
			sb.append(this.s3URI.toString());
			if (this.ioFile != null) {
				sb.append(" (")
					.append(this.ioFile.getAbsolutePath())
					.append(")");
			}
		} else if (this.ioFile != null) {
			sb.append(this.ioFile.getAbsolutePath());
		}

		return sb.toString();
	}

	@Override
	public int compareTo(FileWrapper other) {
		// Both S3 URI are not null, compare them
		if (this.s3URI != null && other.s3URI != null) {
			return this.s3URI.toString().compareTo(other.s3URI.toString());
		}

		// Both File are not null, compare their absolute path
		if (this.ioFile != null && other.ioFile != null) {
			return this.ioFile.getAbsolutePath().compareTo(other.ioFile.getAbsolutePath());
		}

		// Only "this" has a S3 URI, put "this" before "other"
		if (this.s3URI != null) {
			return -1;
		}
		if (other.s3URI != null) {
			return 1;
		}

		// Only "this" has a file, put "this" before "other"
		if (this.ioFile != null) {
			return -1;
		}
		if (other.ioFile != null) {
			return 1;
		}

		// S3 URI and File are null on both object.
		// This should not happen.
		// Just in case, return 1.
		// Do not return 0, we don't want them to be considered equals.
		return 1;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other == null || this.getClass() != other.getClass()) {
			return false;
		}

		FileWrapper otherWrapper = (FileWrapper) other;

		String s3Str = this.s3URI == null ? null : this.s3URI.toString(),
			otherS3Str = otherWrapper.s3URI == null ? null : otherWrapper.s3URI.toString(),
			ioPath = this.ioFile == null ? null : this.ioFile.getAbsolutePath(),
			otherIoPath = otherWrapper.ioFile == null ? null : otherWrapper.ioFile.getAbsolutePath();

		return Objects.equals(s3Str, otherS3Str) &&
				Objects.equals(ioPath, otherIoPath);
	}

	@Override
	public int hashCode() {
		String s3Str = this.s3URI == null ? null : this.s3URI.toString(),
			ioPath = this.ioFile == null ? null : this.ioFile.getAbsolutePath();

		return Objects.hash(s3Str, ioPath);
	}
}
