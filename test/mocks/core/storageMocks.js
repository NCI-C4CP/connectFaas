const sinon = require('sinon');

/**
 * Firebase Storage mocks
 */
class StorageMocks {
    constructor() {
        this.reset();
    }

    reset() {
        if (this.sandbox) {
            this.sandbox.restore();
        }
        this.sandbox = sinon.createSandbox();
        this.createBaseMocks();
    }

    createBaseMocks() {
        // Mock File
        this.mockFile = {
            name: 'test-file.txt',
            bucket: 'test-bucket',
            download: sinon.stub().resolves([Buffer.from('test content')]),
            save: sinon.stub().resolves(),
            delete: sinon.stub().resolves(),
            exists: sinon.stub().resolves([true]),
            getMetadata: sinon.stub().resolves([{
                name: 'test-file.txt',
                size: 12,
                contentType: 'text/plain',
                timeCreated: new Date().toISOString(),
                updated: new Date().toISOString()
            }]),
            setMetadata: sinon.stub().resolves(),
            copy: sinon.stub().resolves(),
            move: sinon.stub().resolves(),
            createReadStream: sinon.stub(),
            createWriteStream: sinon.stub(),
            getSignedUrl: sinon.stub().resolves(['https://example.com/signed-url'])
        };

        // Mock Bucket
        this.mockBucket = {
            name: 'test-bucket',
            file: sinon.stub().returns(this.mockFile),
            getFiles: sinon.stub().resolves([[this.mockFile]]),
            upload: sinon.stub().resolves([this.mockFile]),
            deleteFiles: sinon.stub().resolves(),
            exists: sinon.stub().resolves([true]),
            getMetadata: sinon.stub().resolves([{
                name: 'test-bucket',
                location: 'US',
                storageClass: 'STANDARD'
            }])
        };

        // Mock Storage
        this.mockStorage = {
            bucket: sinon.stub().returns(this.mockBucket),
            getBuckets: sinon.stub().resolves([[this.mockBucket]]),
            createBucket: sinon.stub().resolves([this.mockBucket])
        };
    }

    setupFile(filePath, content = 'test content') {
        const mockFile = {
            ...this.mockFile,
            name: filePath,
            download: sinon.stub().resolves([Buffer.from(content)])
        };
        
        this.mockBucket.file.withArgs(filePath).returns(mockFile);
        return mockFile;
    }

    setupFileError(filePath, error) {
        const mockFile = {
            ...this.mockFile,
            name: filePath,
            download: sinon.stub().rejects(error),
            exists: sinon.stub().resolves([false])
        };
        
        this.mockBucket.file.withArgs(filePath).returns(mockFile);
        return mockFile;
    }

    setupBucket(bucketName) {
        const mockBucket = {
            ...this.mockBucket,
            name: bucketName
        };
        
        this.mockStorage.bucket.withArgs(bucketName).returns(mockBucket);
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