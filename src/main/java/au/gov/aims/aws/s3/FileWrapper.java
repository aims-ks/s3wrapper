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

/**
 * This class can be used to handle both io.File and S3File.
 */
public class FileWrapper {
	private AmazonS3URI s3URI;
	private File ioFile;

	public FileWrapper(AmazonS3URI s3URI, File ioFile) {
		this.ioFile = ioFile;
		this.s3URI = s3URI;
	}

	public FileWrapper(FileWrapper parent, String pathname) {
		this.ioFile = new File(parent.ioFile, pathname);
		this.s3URI = S3Utils.getS3URI(parent.s3URI.getBucket(), parent.s3URI.getKey() + "/" + pathname);
	}

	public FileWrapper getParent() {
		return new FileWrapper(S3Utils.getParentUri(this.s3URI), this.ioFile.getParentFile());
	}

	public boolean isDirectory() {
		if (this.s3URI != null) {
			return this.s3URI.getKey().endsWith("/");
		}

		return this.ioFile.isDirectory();
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
		if (client != null && this.s3URI != null) {
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
		if (client != null && this.s3URI != null) {
			DownloadManager.download(client, this.s3URI, this.ioFile);
		}
	}


	public void uploadFile(S3Client client) throws IOException, InterruptedException {
		if (client != null && this.s3URI != null) {
			UploadManager.upload(client, this.ioFile, this.s3URI);
		}
	}


	public FileWrapper[] listFiles(S3Client client) {
		return this.listFiles(client, null, null);
	}

	public FileWrapper[] listFiles(S3Client client, FilenameFilter filenameFilter) {
		return this.listFiles(client, filenameFilter, null);
	}

	public FileWrapper[] listFiles(S3Client client, FileFilter fileFilter) {
		return this.listFiles(client, null, fileFilter);
	}

	private FileWrapper[] listFiles(S3Client client, FilenameFilter filenameFilter, FileFilter fileFilter) {
		FileWrapper[] fileWrappers = null;

		if (client == null || this.s3URI == null) {
			File[] files;
			if (filenameFilter != null) {
				files = this.ioFile.listFiles(filenameFilter);
			} else if (fileFilter != null) {
				files = this.ioFile.listFiles(fileFilter);
			} else {
				files = this.ioFile.listFiles();
			}

			if (files != null && files.length > 0) {
				fileWrappers = new FileWrapper[files.length];

				for (int i=0; i<files.length; i++) {
					fileWrappers[i] = new FileWrapper(null, files[i]);
				}
			}
		} else {
			S3List s3List;
			if (filenameFilter != null) {
				s3List = ListManager.ls(client, this.s3URI, filenameFilter);
			} else if (fileFilter != null) {
				s3List = ListManager.ls(client, this.s3URI, fileFilter);
			} else {
				s3List = ListManager.ls(client, this.s3URI);
			}

			fileWrappers = this.toFileWrapperArray(s3List);
		}

		return fileWrappers;
	}


	private FileWrapper[] toFileWrapperArray(S3List s3List) {
		if (s3List == null) {
			return null;
		}

		List<FileWrapper> fileWrapperList = new ArrayList<FileWrapper>();

		Map<String, S3File> dirs = s3List.getDirs();
		for (S3File s3File : dirs.values()) {
			AmazonS3URI directoryUri = s3File.getS3Uri();
			String directoryName = S3Utils.getDirectoryName(directoryUri);

			fileWrapperList.add(new FileWrapper(directoryUri, new File(this.ioFile, directoryName)));
		}

		Map<String, S3File> files = s3List.getFiles();
		for (S3File s3File : files.values()) {
			AmazonS3URI fileUri = s3File.getS3Uri();
			String filename = S3Utils.getFilename(fileUri);

			fileWrapperList.add(new FileWrapper(fileUri, new File(this.ioFile, filename)));
		}

		return fileWrapperList.toArray(new FileWrapper[fileWrapperList.size()]);
	}
}
