<?php

declare(strict_types=1);

namespace Doctrine\ODM\MongoDB;

use MongoDB\GridFS\Bucket;

class GridFSRepository extends DocumentRepository
{
    /**
     * Writes the contents of a GridFS file to a writable stream.
     *
     * @param mixed    $id          File ID
     * @param resource $destination Writable Stream
     */
    public function downloadToStream($id, $destination)
    {
        $this->getDocumentBucket()->downloadToStream($id, $destination);
    }

    /**
     * Opens a writable stream for writing a GridFS file.
     *
     * @param object|null $metadata
     * @return resource
     */
    public function openUploadStream(string $filename, $metadata = null)
    {
        $options = $this->prepareMetadataOptions($metadata);

        return $this->getDocumentBucket()->openUploadStream($filename, $options);
    }

    /**
     * Writes the contents of a readable stream to a GridFS file.
     *
     * @param resource    $source   Readable stream
     * @param object|null $metadata
     * @return object The newly created GridFS file
     */
    public function uploadFromStream($filename, $source, $metadata = null)
    {
        $options = $this->prepareMetadataOptions($metadata);

        $id = $this->getDocumentBucket()->uploadFromStream($filename, $source, $options);

        // TODO: apply primary read preference

        return $this->find($id);
    }

    private function getDocumentBucket(): Bucket
    {
        return $this->dm->getDocumentBucket($this->documentName);
    }

    /**
     * @param object|null $metadata
     *
     * @return array
     */
    private function prepareMetadataOptions($metadata = null): array
    {
        if ($metadata === null) {
            return [];
        }

        return ['metadata' => $this->uow->getPersistenceBuilder()->prepareInsertData($metadata)];
    }
}
