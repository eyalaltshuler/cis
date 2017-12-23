import wikipedia
import time

NEWLINE = '\n'
WIKI_OUTPUT_FILE = 'wiki20.txt'
PROGRESS = 10 * 1024 ** 3


def MBytesToBytes(size):
    return size * 1024 ** 3


def BytesToMBytes(size):
    return size / 1024 ** 3


def get_wikipedia_random_pages(size_mbytes):
    print 'Starting download random pages from wikipedia, writing to file %s' % WIKI_OUTPUT_FILE
    output_file = open(WIKI_OUTPUT_FILE, 'a')
    curr_progress = 0
    curr_size = 0
    iters = 0
    data_size_iter = 0
    size_bytes = MBytesToBytes(size_mbytes)
    start = time.time()
    while curr_size < size_bytes:
        try:
            print 'Starting iteration %d ' % iters
            itemset = wikipedia.page(wikipedia.random(pages=1)).content.encode('utf-8').lower().split(" ")
        except:
            continue
        itemset_hashes = [str(hash(word)) for word in itemset]
        itemset_line = " ".join(itemset_hashes) + NEWLINE
        data_size_iter += len(itemset_line)
        curr_size += len(itemset_line)
        iters += 1
        output_file.write(itemset_line)
        if curr_size > curr_progress + PROGRESS:
            print 'Already written %d bytes' % curr_size
        if iters % 10 == 0:
            end = time.time()
            print 'Current 10 iterations took %d seconds and %s bytes collected' % ((end - start), data_size_iter)
            data_size_iter = 0
            start = time.time()
    output_file.close()


if __name__ == "__main__":
    get_wikipedia_random_pages(30)



