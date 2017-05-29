RC IX_ScanIterator::initialize(IXFileHandle &fh, Attribute attribute, const void *low, const void *high, bool lowInc, bool highInc)
{
    // Store all parameters because we will need them later
    attr = attribute;
    fileHandle = &fh;
    lowKey = low;
    highKey = high;
    lowKeyInclusive = lowInc;
    highKeyInclusive = highInc;

    // Initialize our storage
    page = malloc(PAGE_SIZE);
    if (page == NULL)
        return IX_MALLOC_FAILED;
    // Initialize starting slot number
    slotNum = 0;

    // Find the starting page
    IndexManager *im = IndexManager::instance();
    int32_t startPageNum;
    RC rc = im->find(*fileHandle, attr, lowKey, startPageNum);
    if (rc)
    {
        free(page);
        return rc;
    }
    rc = fileHandle->readPage(startPageNum, page);
    if (rc)
    {
        free(page);
        return rc;
    }

    // Find the starting entry
    LeafHeader header = im->getLeafHeader(page);
    int i = 0;
    for (i = 0; i < header.entriesNumber; i++)
    {
        int cmp = (low == NULL ? -1 : im->compareLeafSlot(attr, lowKey, page, i));
        if (cmp < 0)
            break;
        if (cmp == 0 && lowKeyInclusive)
            break;
        if (cmp > 0)
            continue;
    }
    slotNum = i;
    return SUCCESS;
}

RC IndexManager::find(IXFileHandle &handle, const Attribute attr, const void *key, int32_t &resultPageNum)
{
    int32_t rootPageNum;
    RC rc = getRootPageNum(handle, rootPageNum);
    if (rc)
        return rc;
    return treeSearch(handle, attr, key, rootPageNum, resultPageNum);
}

RC IndexManager::getRootPageNum(IXFileHandle &fileHandle, int32_t &result) const
{
    void *metaPage = malloc(PAGE_SIZE);
    if (metaPage == NULL)
        return IX_MALLOC_FAILED;
    RC rc = fileHandle.readPage(0, metaPage);
    if (rc)
    {
        free(metaPage);
        return IX_READ_FAILED;
    }

    MetaHeader header = getMetaData(metaPage);
    free(metaPage);
    result = header.rootPage;
    return SUCCESS;
}

MetaHeader IndexManager::getMetaData(const void *pageData) const
{
    MetaHeader header;
    memcpy(&header, pageData, sizeof(MetaHeader));
    return header;
}