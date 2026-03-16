/**
 * Firebase Storage mocks
 */
class StorageMocks {
    constructor() {
        this.reset();
    }

    reset() {
        this._fileOverrides = new Map();
        this._bucketOverrides = new Map();
        this.createBaseMocks();
    }

    createBaseMocks() {
        // Mock File
        this.mockFile = {
            name: 'test-file.txt',
            bucket: 'test-bucket',
            download: vi.fn().mockResolvedValue([Buffer.from('test content')]),
            save: vi.fn().mockResolvedValue(undefined),
            delete: vi.fn().mockResolvedValue(undefined),
            exists: vi.fn().mockResolvedValue([true]),
            getMetadata: vi.fn().mockResolvedValue([{
                name: 'test-file.txt',
                size: 12,
                contentType: 'text/plain',
                timeCreated: new Date().toISOString(),
                updated: new Date().toISOString()
            }]),
            setMetadata: vi.fn().mockResolvedValue(undefined),
            copy: vi.fn().mockResolvedValue(undefined),
            move: vi.fn().mockResolvedValue(undefined),
            createReadStream: vi.fn(),
            createWriteStream: vi.fn(),
            getSignedUrl: vi.fn().mockResolvedValue(['https://example.com/signed-url'])
        };

        const self = this;

        // Mock Bucket
        this.mockBucket = {
            name: 'test-bucket',
            file: vi.fn().mockImplementation((path) => self._fileOverrides.get(path) || self.mockFile),
            getFiles: vi.fn().mockResolvedValue([[this.mockFile]]),
            upload: vi.fn().mockResolvedValue([this.mockFile]),
            deleteFiles: vi.fn().mockResolvedValue(undefined),
            exists: vi.fn().mockResolvedValue([true]),
            getMetadata: vi.fn().mockResolvedValue([{
                name: 'test-bucket',
                location: 'US',
                storageClass: 'STANDARD'
            }])
        };

        // Mock Storage
        this.mockStorage = {
            bucket: vi.fn().mockImplementation((name) => self._bucketOverrides.get(name) || self.mockBucket),
            getBuckets: vi.fn().mockResolvedValue([[this.mockBucket]]),
            createBucket: vi.fn().mockResolvedValue([this.mockBucket])
        };
    }

    setupFile(filePath, content = 'test content') {
        const mockFile = {
            ...this.mockFile,
            name: filePath,
            download: vi.fn().mockResolvedValue([Buffer.from(content)])
        };

        this._fileOverrides.set(filePath, mockFile);
        return mockFile;
    }

    setupFileError(filePath, error) {
        const mockFile = {
            ...this.mockFile,
            name: filePath,
            download: vi.fn().mockRejectedValue(error),
            exists: vi.fn().mockResolvedValue([false])
        };

        this._fileOverrides.set(filePath, mockFile);
        return mockFile;
    }

    setupBucket(bucketName) {
        const mockBucket = {
            ...this.mockBucket,
            name: bucketName
        };

        this._bucketOverrides.set(bucketName, mockBucket);
        return mockBucket;
    }

    getMocks() {
        return {
            storage: this.mockStorage,
            bucket: this.mockBucket,
            file: this.mockFile
        };
    }
}

module.exports = StorageMocks;
