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
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class can be used to handle both io.File and S3File.
 */
public class FileWrapper implements Comparable<FileWrapper> {
    private static final Logger LOGGER = Logger.getLogger(FileWrapper.class);

    private URI uri;
    private File ioFile;

    // Flag to monitor if the file was downloaded from S3 or uploaded to S3
    private boolean downloaded = false;
    private boolean uploaded = false;

    public FileWrapper(AmazonS3URI s3URI, File ioFile) {
        this.ioFile = ioFile;
        this.uri = s3URI.getURI();
    }

    /**
     * Allow file:// and s3:// URIs
     * @param uri
     * @param ioFile
     */
    public FileWrapper(URI uri, File ioFile) {
        this.ioFile = ioFile;
        if (uri != null && uri.getScheme() == null) {
            try {
                this.uri = new URI("file://" + uri.toString());
            } catch(Exception ex) {
                LOGGER.error("Invalid file URI: " + uri, ex);
                this.uri = uri;
            }
        } else {
            this.uri = uri;
        }
    }

    public FileWrapper(FileWrapper parent, String pathname) throws URISyntaxException {
        this.ioFile = parent.ioFile == null ? null :
                new File(parent.ioFile, pathname);

        this.uri = null;
        if (parent.uri != null) {
            String childPath = pathname;

            String parentPath = parent.uri.getPath();
            if (parentPath != null && !parentPath.isEmpty()) {
                if (!parentPath.endsWith("/")) {
                    parentPath += "/";
                }
                childPath = parentPath + pathname;
            }

            this.uri = new URI(parent.uri.getScheme(), parent.uri.getHost(), childPath, null);
        }
    }

    public FileWrapper getParent() throws URISyntaxException {
        return new FileWrapper(
                URIUtils.getParentUri(this.uri),
                this.ioFile == null ? null : this.ioFile.getParentFile());
    }

    public boolean isDirectory() {
        if (this.uri != null) {
            return this.uri.getPath().endsWith("/");
        }

        if (this.ioFile != null) {
            return this.ioFile.isDirectory();
        }

        return false;
    }

    public boolean exists(S3Client client) {
        if (this.uri != null) {
            String scheme = this.uri.getScheme();
            if ("s3".equals(scheme)) {
                if (client == null) {
                    return false;
                }
                AmazonS3URI s3URI = new AmazonS3URI(this.uri);
                return client.getS3().doesObjectExist(s3URI.getBucket(), s3URI.getKey());
            } else if ("file".equals(scheme)) {
                return new File(this.uri).exists();
            }
        }

        if (this.ioFile != null) {
            return this.ioFile.exists();
        }

        return false;
    }

    public String getFilename() {
        if (this.uri != null) {
            return URIUtils.getFilename(this.uri);
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
        if (this.uri != null && "s3".equals(this.uri.getScheme())) {
            return new AmazonS3URI(this.uri);
        }

        return null;
    }

    public URI getURI() {
        return this.uri;
    }

    public S3File getS3File(S3Client client) {
        if (this.uri != null && this.ioFile != null) {
            if ("s3".equals(this.uri.getScheme())) {
                if (client != null) {
                    AmazonS3URI s3URI =  new AmazonS3URI(this.uri);
                    S3List s3List = ListManager.ls(client, s3URI);
                    return s3List.getFiles().get(s3URI.getKey());
                }
            }
        }

        return null;
    }

    public Long getS3LastModified(S3Client client) {
        if (this.uri != null) {
            String scheme = this.uri.getScheme();
            if ("s3".equals(scheme)) {
                if (client != null) {
                    S3File s3File = this.getS3File(client);
                    if (s3File != null) {
                        return s3File.getLastModified();
                    }
                }
            } else if ("file".equals(scheme)) {
                File s3File = new File(this.uri);
                if (s3File.exists()) {
                    return s3File.lastModified();
                }
            }
        }

        return null;
    }

    public Long getLastModified(S3Client client) {
        if (this.isOriginalOnDisk()) {
            return this.ioFile.lastModified();
        }

        return this.getS3LastModified(client);
    }

    public boolean isOutdated(S3Client client) {
        if (this.uri == null || this.ioFile == null) {
            return false;
        }

        if (!this.ioFile.exists()) {
            return true;
        }

        Long s3LastModified = this.getS3LastModified(client);
        if (s3LastModified == null) {
            return false;
        }

        return this.ioFile.lastModified() < s3LastModified;
    }

    public File downloadFile(S3Client client) throws IOException {
        return this.downloadFile(client, false);
    }

    public File downloadFile(S3Client client, boolean forceDownload) throws IOException {
        if (this.uri != null && this.ioFile != null) {
            boolean downloadedNeeded = false;

            if (forceDownload) {
                downloadedNeeded = true;
            } else {
                // If there is no filename, the S3URI denote a folder
                //   (we won't download a whole folder)
                String filename = URIUtils.getFilename(this.uri);
                if (filename != null && !filename.isEmpty()) {
                    downloadedNeeded = this.isOutdated(client);
                }
            }

            if (downloadedNeeded) {
                String scheme = this.uri.getScheme();
                if ("s3".equals(scheme)) {
                    if (client != null) {
                        AmazonS3URI s3URI =  new AmazonS3URI(this.uri);
                        if (!client.getS3().doesObjectExist(s3URI.getBucket(), s3URI.getKey())) {
                            return null;
                        }
                    }
                } else if ("file".equals(scheme)) {
                    File file = new File(this.uri);
                    if (!file.exists()) {
                        return null;
                    }
                }
                this.forceDownloadFile(client);
            }
        }

        return this.ioFile;
    }

    private void forceDownloadFile(S3Client client) throws IOException {
        if (this.uri != null && this.ioFile != null) {
            String scheme = this.uri.getScheme();
            if ("s3".equals(scheme)) {
                if (client != null) {
                    AmazonS3URI s3URI =  new AmazonS3URI(this.uri);
                    DownloadManager.download(client, s3URI, this.ioFile);
                    this.downloaded = true;
                }
            } else if ("file".equals(scheme)) {
                File file = new File(this.uri);
                FileUtils.copyFile(file, this.ioFile);
                this.downloaded = true;
            }
        }
    }

    public boolean isOriginalOnS3() {
        if (this.uri == null) {
            return false;
        }

        if (this.ioFile == null) {
            return true;
        }

        return !this.ioFile.exists() || this.downloaded || this.uploaded;
    }

    public boolean isOriginalOnDisk() {
        if (this.ioFile == null) {
            return false;
        }

        return !this.isOriginalOnS3();
    }

    /**
     * Delete the file if it was downloaded from S3.
     * @return True if the file was deleted from disk. False otherwise.
     */
    public boolean cleanup() {
        if (this.isOriginalOnS3() && this.ioFile != null && this.ioFile.exists()) {
            return this.ioFile.delete();
        }

        return false;
    }

    public void uploadFile(S3Client client) throws IOException, InterruptedException {
        if (this.uri != null && this.ioFile != null) {
            String scheme = this.uri.getScheme();
            if ("s3".equals(scheme)) {
                if (client != null) {
                    AmazonS3URI s3URI =  new AmazonS3URI(this.uri);
                    UploadManager.upload(client, this.ioFile, s3URI);
                    this.uploaded = true;
                }
            } else if ("file".equals(scheme)) {
                File file = new File(this.uri);
                File directory = file.getParentFile();
                directory.mkdirs();
                FileUtils.copyFile(this.ioFile, file);
                this.uploaded = true;
            }
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

        if (this.uri != null) {
            String scheme = this.uri.getScheme();
            if ("s3".equals(scheme)) {
                if (client != null) {
                    AmazonS3URI s3URI =  new AmazonS3URI(this.uri);

                    S3List s3List;
                    if (filenameFilter != null) {
                        s3List = ListManager.ls(client, s3URI, filenameFilter, recursive);
                    } else if (fileFilter != null) {
                        s3List = ListManager.ls(client, s3URI, fileFilter, recursive);
                    } else {
                        s3List = ListManager.ls(client, s3URI, recursive);
                    }

                    if (s3List != null) {
                        fileWrappers = this.toFileWrapperList(s3List);
                    }
                }
            } else if ("file".equals(scheme)) {
                File file = new File(this.uri);
                fileWrappers = this.listFiles(file, filenameFilter, fileFilter, recursive);
            }

        } else if (this.ioFile != null) {
            fileWrappers = this.listFiles(this.ioFile, filenameFilter, fileFilter, recursive);
        }

        return fileWrappers;
    }

    private List<FileWrapper> listFiles(File file, FilenameFilter filenameFilter, FileFilter fileFilter, boolean recursive) {
        List<FileWrapper> fileWrappers = null;

        File[] files;
        if (filenameFilter != null) {
            files = file.listFiles(filenameFilter);
        } else if (fileFilter != null) {
            files = file.listFiles(fileFilter);
        } else {
            files = file.listFiles();
        }

        if (files != null && files.length > 0) {
            fileWrappers = new ArrayList<FileWrapper>(files.length);

            for (File childFile : files) {
                fileWrappers.add(new FileWrapper((URI)null, childFile));
            }


            if (recursive) {
                // List all directories
                // NOTE: This is not optimal but that seems
                //   to be the best way to do it in java:
                //   - https://stackoverflow.com/questions/5125242/java-list-only-subdirectories-from-a-directory-not-files
                //   - https://stackoverflow.com/questions/1034977/how-to-retrieve-a-list-of-directories-quickly-in-java
                File[] dirs = file.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File parent, String filename) {
                        return new File(parent, filename).isDirectory();
                    }
                });

                if (dirs != null) {
                    for (File dir : dirs) {
                        FileWrapper dirFileWrapper = new FileWrapper((URI)null, dir);
                        fileWrappers.addAll(
                                dirFileWrapper.listFiles(dir, filenameFilter, fileFilter, recursive));
                    }
                }
            }
        }

        return fileWrappers;
    }

    private List<FileWrapper> toFileWrapperList(S3List s3List) {
        List<FileWrapper> fileWrapperList = new ArrayList<FileWrapper>();

        if (s3List != null) {
            AmazonS3URI s3URI = this.uri == null ? null : new AmazonS3URI(this.uri);
            String thisKey = s3URI == null ? "" : s3URI.getKey();
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

        if (this.uri != null) {
            sb.append(this.uri.toString());
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
        if (this.uri != null && other.uri != null) {
            return this.uri.toString().compareTo(other.uri.toString());
        }

        // Both File are not null, compare their absolute path
        if (this.ioFile != null && other.ioFile != null) {
            return this.ioFile.getAbsolutePath().compareTo(other.ioFile.getAbsolutePath());
        }

        // Only "this" has a S3 URI, put "this" before "other"
        if (this.uri != null) {
            return -1;
        }
        if (other.uri != null) {
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

        String s3Str = this.uri == null ? null : this.uri.toString(),
                otherS3Str = otherWrapper.uri == null ? null : otherWrapper.uri.toString(),
                ioPath = this.ioFile == null ? null : this.ioFile.getAbsolutePath(),
                otherIoPath = otherWrapper.ioFile == null ? null : otherWrapper.ioFile.getAbsolutePath();

        return Objects.equals(s3Str, otherS3Str) &&
                Objects.equals(ioPath, otherIoPath);
    }

    @Override
    public int hashCode() {
        String s3Str = this.uri == null ? null : this.uri.toString(),
                ioPath = this.ioFile == null ? null : this.ioFile.getAbsolutePath();

        return Objects.hash(s3Str, ioPath);
    }
}
