**This is fork of the original [bigdataviewer/bigdataviewer-server](https://github.com/bigdataviewer/bigdataviewer-server) repository.**

# Web Server for BigDataViewer datasets

## Basic Web Server
Serves one or more XML/HDF5 datasets for remote access over HTTP. Provide (NAME XML) pairs on the command line or in a dataset file, 
where NAME is the name under which the dataset should be made accessible and XML is the path to the XML file of the dataset.

### Command line options
- `-b <BASEURL>`    - Base URL under which the server will be made visible (e.g., if behind a proxy).
- `-d <FILE>`       - Dataset file: A plain text file specifying one dataset per line. Each line is formatted as "NAME `<TAB>` XML".
- `-p <PORT>`       - Listening port (default: 8080).
- `-s <HOSTNAME>`   - Hostname of the server (default: `<COMPUTER-NAME>`).
- `-t <DIRECTORY>`  - Directory to store thumbnails (new temporary directory by default.).

## QCMP Compression Extension
The QCMP compression extension of BigDataViewer Web Server gives user an option to use lossy compression, which greatly reduce the amount of data transfered through the network. Vector quantization with Huffman coding is used to compress the data.

Enabling this extension doesn't limit the basic web server functionality in any way. Requests to uncompressed data are sent with `cell` parameter, while requests to compressed data to `cell_qcmp`.

In order to be able to use compression for your dataset, the dataset codebook for given quantization must be prepared. The codebook can be created with the QCMP console application using the `-tcb,--train-codebook` CLI method. For further details see [compression library source code and README](https://code.it4i.cz/BioinformaticDataCompression/QcmpCompressionLibrary). The codebooks are required to be created before hand, because their creation is quite time consuming.

### Additional command line options
- `-qcmp`                               - Enables the QCMP compression on the server.
- `-cbc, --codebook-cache <DIRECTORY>`  - Directory with prepared codebooks.
- `-vq, --vector-quantization <VECTOR>` - Set vector quantization as the used lossy compression algorithm. *REQUIRED FOR NOW*. `VECTOR` specifies the dimenions of the quantization vector eg. 3x3, 3x3x3, ...
- `-v, --verbose`                       - Make the QCMP cell handler verbose.
- `-wc, --worker-count`                 - Number of worker threads used for codebook training.
- `-cf,--compress-from <PYRAMID-LEVEL>` - Set minimal mipmap/pyramid level, which should be compressed. For example `-cf 1` means that level 0 won't be compressed, but levels greater and equal to 1 will be.
- `tcb` - Specify after dataset pair, to enable codebook training on the server startup.


### Note about `tcb`:
`tcb` option can be specified in both dataset file and command line arguments. In the dataset file, it must be separated by `<tab>` character, same as dataset name and xml file path.
The trained codebooks will be saved in the codebook cache directory, which is specified by the `-cbc` options. Codebooks will be read from this directory in the next run of the server application.


### Example:
We have dataset *drosophila32* with its XML file drosophila_32x32.xml and we want to use vector quantization with quantization vector of dimensions 3x3x3. This is three-dimensional *voxel* vector with 27 values. We have exported our dataset using BigDataViewer's `Export Current Image as XML/HDF5` tool. We recommend to check `manual mipmap setup` and set bigger Hdf5 chunk sizes, for example we have used {32,32,32} for all mipmap levels. Bigger chunks would normally result in greater data transfer (8 KB for 16^3 and 65 KB for 32^3), but compression greatly reduces the chunk size. Also we recommend to create as many codebooks as there are subsampling factors or pyramid levels. By multiple codebooks we mean codebooks of different size. Larger codebook means better compression. Possible codebook sizes are (*for now*) 256, 128, 64, 32, 16, 8 and 4. If we export our dataset with four pyramid levels/subsampling factors, four largest codebooks would be used 256, 128, 64 and 32.

Subsampling factor / pyramid level mapping to codebook size example:
```
Level 0 --> Codebook size 256 (Best quality)
Level 1 --> Codebook size 128
Level 2 --> Codebook size 64
Level 3 --> Codebook size 32
```

If we choose to set `--compress-from 1` the mapping changes accordingly:
```
Level 0 --> Uncompressed data
Level 1 --> Codebook size 256
Level 2 --> Codebook size 128
Level 3 --> Codebook size 64
```
To start a local server with enabled QCMP compression we would then use these options:
```
java -ea -jar bigdataviewer-server.jar -s 127.0.0.1 -p 8080 drosophila32 D:\..\..\drosophila_32x32.xml -qcmp -vq 3x3x3 -cbc D:\..\..\dc_cache_huffman\ -cf 1
```

In the dc_cache_huffman directory we can find following codebook files. Dataset name **must** match with the prefix of the codebook file.
```
drosophila_32x32_256_3x3x3.qvc
drosophila_32x32_128_3x3x3.qvc
drosophila_32x32_64_3x3x3.qvc
drosophila_32x32_32_3x3x3.qvc
drosophila_32x32_16_3x3x3.qvc
drosophila_32x32_8_3x3x3.qvc
drosophila_32x32_4_3x3x3.qvc
```
