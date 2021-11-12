import sys

__all__ = (
        'Logger',
        )

# ---------------------------------------------------------------------------

class Logger:
    """
    Simple class for outputing messages to a log.  It also keeps track
    of a specified number of previous messages to determine if lines are
    repeating.
    
    """

    def __init__(self, fhandler=None, showTime=1, maxrepeat=0,
                 threadsafe=0):
        """
        Specifiy the file handler to write the messages to.  If no
        file handler is provided then use stdout.

        @param fhandler:    (default None)
        @param showTime:    (default True)
        @param maxrepeat:   (default 0)
        @param threadsafe:  (default False)
        
        """
        
        if not fhandler:
            import sys
            self.file = sys.stdout

        self.showTime    = showTime
        self.maxrepeat   = maxrepeat
        self.msglist     = []
        self.matchstart  = -1
        self.matchend    = -1
        self.singlematch = 0
        self.multimatch  = 0
        self.matchlength = 0
        self.threadsafe  = threadsafe
        if threadsafe:
            import threading
            self.msglock = threading.Lock()


    def printmsgs(self, start, end=-1):
        """
        Print the messages that are currently in the msglist from
        start to end and then clear them from the list.

        @param start:
        @type start: integer
        @param end:
        @type end: integer
        
        """

        if end < 0:
            msgtime,msg = self.msglist[start]
            self.file.write('%s => %s\n' % (msgtime, msg))
            self.file.flush()
            #self.file.write('%s\n' % (msg))
        else:
            for i in range(start, end + 1):
                msgtime,msg = self.msglist[i]
                self.file.write('%s => %s\n' % (msgtime, msg))
                self.file.flush()
                #self.file.write('%s\n' % (msg))


    def reset(self):
        """
        Reset the current repeat numbers and print a repeat message.
        
        """

        repmsg = '[the last %d line%s repeated %d time%s]\n'
        # first check if we should print a repeat message
        if self.singlematch:                
            # if this is a single line repeated once then just print the
            # line
            if self.singlematch == 1:
                self.printmsgs(0)
            else:
                self.file.write(repmsg % (1, '', self.singlematch, 's'))
                self.file.flush()
        elif self.multimatch:
            if self.multimatch > 1:
                plural = 's'
            else:
                plural = ''
            self.file.write(repmsg % (self.matchlength, 's',
                                      self.multimatch, plural))
            self.file.flush()

        # check for an unsuccessful multiline match and clear the old
        # messages
        if self.matchend >= 0 and \
           self.matchend != len(self.msglist) - 1:
            #print 'resting incomplete multi match'
            self.printmsgs(0, self.matchend)
            # if the multiline match was successful before then remove the
            # messages
            if self.multimatch:
                self.msglist = self.msglist[0:(self.matchend + 1)]
            # swith the order of the messages
            else:
                prevmatch    = self.msglist[0:(self.matchend + 1)]
                self.msglist = self.msglist[(self.matchend + 1):]
                self.msglist.extend(prevmatch)
        elif self.singlematch or self.multimatch:
            self.msglist = []
            
        self.singlematch = 0
        self.multimatch  = 0
        self.matchlength = 0
        self.matchstart  = -1
        self.matchend    = -1


    # alternate name for reset
    finish = reset


    def addMessage(self, msg):
        """
        Add a message to the buffer and print it.
        
        @param msg:
        
        """

        self.reset()

        # check if we should remove a message from the front.
        if len(self.msglist) == self.maxrepeat:
            self.msglist.pop(0)
        self.msglist.append((time.ctime(), msg))
        self.printmsgs(len(self.msglist) - 1)


    def matchFound(self, index, msg):
        """
        Check if the match found at index i is a continuation or not.
        
        @param index:
        @param msg:
        
        """
        #print index, self.msglist

        # if no other match has been found then set some numbers
        if self.matchstart < 0:
            #print self.multimatch, index
            # if we just finished with a multiline match and now we are
            # matching a line within the multiline match, then clear the
            # buffer and don't consider this a match
            if self.multimatch and index != 0:
                self.reset()
                self.msglist = []
                self.addMessage(msg)
                return
            
            # remove all the messages before this match since they can no
            # longer be considered for a match
            self.msglist = self.msglist[index:]

            self.matchstart = 0
            self.matchend   = 0

            # if there is no only one message in the list, then the match
            # will be single line
            if len(self.msglist) == 1:
                self.singlematch = 1

            return

        # now check if this is from a single or multi line match
        # a single line match can only exist at the bottom of the list, and
        # since we delete all messages before the first matched line we can
        # gaurantee that a single line match will have one item in the list
        if self.singlematch:
            self.singlematch += 1
            return

        # if we reach this point then we have multiline match on our hands
        # and things can get tricky
        # the match is wrapping back around too early
        # make sure the current match follows a previous match
        if index != self.matchend + 1:
            self.reset()
            self.addMessage(msg)
        else:
            self.matchend = index
            # check if the the muliline match is complete
            if index == len(self.msglist) - 1:
                self.matchlength = self.matchend - self.matchstart + 1
                self.multimatch += 1
                self.matchstart  = -1
                self.matchend    = -1


    def checkmsg(self, msg):
        """
        Check if the current message is the beginning of a match or the
        continuation of one.
        
        @param msg:
        
        """

        order = list(range(len(self.msglist)))
        order.reverse()

        #print msg, self.msglist
        # search for matches starting from the bottom
        for i in order:
            msgtime,oldmsg = self.msglist[i]
            #print msg, oldmsg
            # we have a match
            if msg == oldmsg:
                # update the time of the incoming message
                self.msglist[i] = (time.ctime(), msg)
                self.matchFound(i, msg)
                break
        # otherwise add the line to the msglist and print it
        else:
            self.addMessage(msg)


    def log(self, msg):
        """
        @param msg:

        """
        
        if self.threadsafe: self.msglock.acquire()
        try:
            if not self.maxrepeat:
                log(msg)
            else:
                self.checkmsg(msg)
        except IndexError:
            import rpg.tracebackutil
            rpg.tracebackutil.printTraceback()
            log(msg)
        except:
            if self.threadsafe: self.msglock.release()
            raise
        if self.threadsafe: self.msglock.release()
