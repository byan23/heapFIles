#include "heapfile.h"
#include "error.h"

#include <stdio.h>
#include "stdlib.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
	status = db.createFile(fileName);	//Create a db level file
	if((status != OK) && (status != FILEEXISTS))
		return status;			//return a BADFILE or UNIXERR
	status = db.openFile(fileName, file);
	if(status != OK) return status;
	hdrPageNo = -1;
	// create a header page
	status = bufMgr->allocPage(file, hdrPageNo, newPage);
	if(status != OK) return status;
	//cast Page* to FileHdrPage*
	hdrPage = (FileHdrPage*)newPage;
	//initialize file name in header page
	strcpy(hdrPage->fileName, fileName.c_str());
	status = bufMgr->allocPage(file, newPageNo, newPage);
	if(status != OK) return status;	
	//invoke init() of the new page
	newPage->init(newPageNo);	
	//update header parameter	
	hdrPage->firstPage = newPageNo;
	hdrPage->lastPage = newPageNo;	
	hdrPage->pageCnt = 1;
	hdrPage->recCnt = 0;
	//unpin both pages and mark them as dirty
	status = bufMgr->unPinPage(file, hdrPageNo, true);
	if(status != OK) return status;
	status = bufMgr->unPinPage(file, newPageNo, true);
	if(status != OK) return status;
	//close the file
	//flush the freshly created file to disk
	status = bufMgr->flushFile(file);
	if(status != OK) return status;
	return (db.closeFile(file));
    }

    status = db.closeFile(file);
    if(status != OK) return status;			//return error if not closed properly
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {	
	headerPageNo = -1;
	//get the header page
	status = filePtr->getFirstPage(headerPageNo);	
	if(status != OK){ 
		returnStatus = status;
		return;
	}
	//read in the header page
	status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
	if(status != OK){
		returnStatus = status;
		return;
	}
	//initialize the headerPage ptr
	headerPage = (FileHdrPage*)pagePtr;
	//initialize the dirty flag
	hdrDirtyFlag = false;		

	status = bufMgr->readPage(filePtr, headerPage->firstPage, pagePtr);
	if(status != OK){
		returnStatus = status;
		return;
	}
	//read success, initialize protected parameters
	curPage = pagePtr;			
	curPageNo = headerPage->firstPage;
	curDirtyFlag = false;
	curRec = NULLRID;
	returnStatus = status;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID &  rid, Record & rec)
{
//	cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    Status 	status;
    Page* 	pagePtr;
	//if desired record in curPage, get it directly
    if(rid.pageNo == curPageNo)
	return (curPage->getRecord(rid, rec));
    else{
	//unpin the current page
	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
	if((status != OK) && (status != PAGENOTPINNED)) return status;
	if(status == PAGENOTPINNED) cout<<"page initially not pinned occurred!"<<endl;
	//read the desired page into bufPool
	status = bufMgr->readPage(filePtr, rid.pageNo, pagePtr);
	if(status != OK) return status;
	//set the new current parameters
	curRec = rid;				
	curPage = pagePtr;
	curPageNo = rid.pageNo;
	curDirtyFlag = false;				
	return (curPage->getRecord(rid, rec));		//update rec

    }
    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        ((type_ == INTEGER && length_ != sizeof(int))
         || (type_ == FLOAT && length_ != sizeof(float))) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
//    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;
    Page*	newPage;

	
    nextPageNo = -1;
    if(curRec.slotNo == -1){
	status = curPage->firstRecord(tmpRid);
    	if(status != OK) return status;
    }
    else{
	status = curPage->nextRecord(curRec, tmpRid);
    }
    //if curPage is not the last page in the file
	while(curPageNo != headerPage->lastPage){
		while(status != ENDOFPAGE){
			//convert curRec to a rec
			curRec = tmpRid;
			status = curPage->getRecord(tmpRid, rec);
			if(status != OK) return status;
			//check if match, if match, return the record
			if(matchRec(rec)){
				curRec = tmpRid;
				outRid = curRec;
				return OK;
			}
			status = curPage->nextRecord(curRec, tmpRid);
			
		}
		do{
		
			//all recidrds on the page have been processed, unpin page
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if(status != OK) return status;
			//get the next page
			status = curPage->getNextPage(nextPageNo);
			if(status != OK) cerr<<"next page error!\n";
			//read in the next page, update cur para
			status = bufMgr->readPage(filePtr, nextPageNo, newPage);
			if(status != OK) return status;
			curPageNo = nextPageNo;
			curPage = newPage;
			curDirtyFlag = false;
			status = curPage->firstRecord(tmpRid);
		} while(status == NORECORDS && curPageNo != headerPage->lastPage);	
			//basically to prevent norecords from being included in desired records
			
		if((status != OK) && (status != NORECORDS)){ cerr<<"more than norecords!"; return status; }
	}
	
    //if curPage is the last page or prev pages are all processed
    while((status != ENDOFPAGE) && (status != NORECORDS)){
	//convert curRec to a rec
	curRec = tmpRid;
	status = curPage->getRecord(tmpRid, rec);
	if(status != OK) return status;
	//check if match
	if(matchRec(rec)){
		curRec = tmpRid;
		outRid = curRec;
		return OK;
	}
	status = curPage->nextRecord(curRec, tmpRid);
    }
    return FILEEOF;
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from fie. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will read the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    //the current page is not the last page of the file, unpin it and bring in the last page
    if(curPageNo != headerPage->lastPage){
	//unpin the current page
	unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
	if(unpinstatus != OK) cerr<<"error in unpin insertion\n";
	//bring in the last page
	status = bufMgr->readPage(filePtr, headerPage->lastPage, newPage);
	if(status != OK) return status;
	//update cur para
	curPage = newPage;
	curPageNo = headerPage->lastPage;
	curDirtyFlag = false;
	curRec = NULLRID;	
    }
    //try to insert
    status = curPage->insertRecord(rec, rid);
    //if full, alloc a new page and insert the record into the new page
    if(status == NOSPACE){
	//unpin the last page
	unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
	if(unpinstatus != OK) cerr<<"error in unpin of data page during insertion\n";
	//alloc a new page in the file
	status = bufMgr->allocPage(filePtr, newPageNo, newPage);
	if(status != OK) return status;
	//init the page
	newPage->init(newPageNo);
	//set the next page parameter to link the new page together
	curPage->setNextPage(newPageNo);
	//update header page and current para
	headerPage->lastPage = newPageNo;
	headerPage->pageCnt++;
	hdrDirtyFlag = true;
	curPage = newPage;
	curPageNo = newPageNo;
	curRec = NULLRID;
	//insert the record to the new page
	status = curPage->insertRecord(rec, rid);
	if(status != OK) cerr<<"New page is full, which is weird!"<<endl;
    }
    outRid = rid;
    curRec = outRid;
    curDirtyFlag = true;
    if((status != OK) && (status != NOSPACE))	cerr<<"There is other status in insertion!";
	//record count addition
    headerPage->recCnt++;

    return OK;
}

