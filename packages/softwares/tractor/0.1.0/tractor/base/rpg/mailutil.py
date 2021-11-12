import types, smtplib, sys
try:
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from eamil.mime.message import MIMEMessage
except ImportError:
    from email.MIMEText import MIMEText
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEMessage import MIMEMessage

from rpg.osutil import getusername

__all__ = (
        'mailserver',
        'maildomain',
        'sendmail',
        )

# ---------------------------------------------------------------------------

mailserver  = 'smtp.example.com'
maildomain  = 'example.com'
MaxPagerMsgLen = 160

def htmlify(txt):
    return "<html><body><pre>" + txt + "</pre></body></html>"

def sendmail(toaddrs, body, fromaddr=None, subject=None,
             replyto=None, multipart_alternative=True):
    """Sends an email to the addresses in toaddrs (can be a string if
    only one address is provided, otherwise must be a list)."""

    def adddomain(addrs):
        """appends the mail domain to each address if it isn't found."""
        if type(addrs) is not type([]):
            addrs = [addrs]
        # add mail domain where needed
        result = [x for x in addrs if "@" in x] + \
                 ["%s@%s" % (x, maildomain) for x in addrs if "@" not in x]
        return result

    if not fromaddr:
        fromaddr = getusername()

    # append the maildomain if need be
    myfromaddr = adddomain(fromaddr)[0]

    if multipart_alternative:
        # multipart mail for mailers
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body))
        msg.attach(MIMEText(htmlify(body), "html"))
    else:
        msg = MIMEText(body)
    # single part for txt msgs
    smsbody = body
    if len(smsbody) > MaxPagerMsgLen:
        smsbody = body[:MaxPagerMsgLen-3] + "..."
    smsmsg = MIMEText(smsbody)

    msg["From"] = myfromaddr
    smsmsg["From"] = myfromaddr

    # get a list of the to addresses
    mytoaddrs  = adddomain(toaddrs)

    # split up the addresses into mailtoaddrs and pagers
    mailtoaddrs = [a for a in mytoaddrs if "-pager" not in a]
    pagers = [a for a in mytoaddrs if "pager" in a]
    
    msg["To"] = ", ".join(mailtoaddrs)
    smsmsg["To"] = ", ".join(pagers)
    
    # add the reply to address
    if replyto:
        msg["Reply-To"] = adddomain(replyto)[0]

    if subject:
        msg["Subject"] = subject
        smsmsg["Subject"] = subject
    
    mail = smtplib.SMTP(mailserver)
    if mailtoaddrs:
        mail.sendmail(myfromaddr, mailtoaddrs, msg.as_string())
    if pagers:
        mail.sendmail(myfromaddr, pagers, smsmsg.as_string())
