'''
  (C) Copyright 2018-2022 Intel Corporation.

  SPDX-License-Identifier: BSD-2-Clause-Patent
'''
import traceback

from apricot import TestWithServers
from pydaos.raw import DaosContainer, DaosApiError


class ObjectPunchTest(TestWithServers):
    """Simple test to verify the 3 different punch calls.
    :avocado: recursive
    """

    def setUp(self):
        super().setUp()
        self.prepare_pool()

        try:
            # create a container
            self.container = DaosContainer(self.context)
            self.container.create(self.pool.pool.handle)

            # now open it
            self.container.open()
        except DaosApiError as excpn:
            self.log.info(excpn)
            self.log.info(traceback.format_exc())
            self.fail("Failed to create and open container")

    def test_dkey_punch(self):
        """The most basic test of the dkey punch function.

        :avocado: tags=all,daily_regression
        :avocado: tags=vm
        :avocado: tags=object,dkey
        :avocado: tags=ObjectPunchTest,test_dkey_punch
        """
        # create an object and write some data into it
        thedata = b"a string that I want to stuff into an object"
        dkey = b"this is the dkey"
        akey = b"this is the akey"
        tx_handle = self.container.get_new_tx()
        self.log.info("Created a new TX for punch dkey test")

        obj = self.container.write_an_obj(
            thedata, len(thedata) + 1, dkey, akey, obj_cls=1, txn=tx_handle)
        self.log.info("Committing the TX for punch dkey test")
        self.container.commit_tx(tx_handle)
        self.log.info("Committed the TX for punch dkey test")

        # read the data back and make sure its correct
        thedata2 = self.container.read_an_obj(
            len(thedata) + 1, dkey, akey, obj, txn=tx_handle)
        if thedata != thedata2.value:
            self.log.info("wrote data: %s", thedata)
            self.log.info("read data:  %s", thedata2.value)
            self.fail("data read doesn't match data written")
        self.log.info("data read matches data written")

        # Punch the data and expect it to fail, since it is not committed
        try:
            obj.punch_dkeys(tx_handle, [dkey])
            self.fail("expected dkey punch to fail")
        except DaosApiError as err:
            self.log.info('dkey punch failed as expected')
            self.log.info(err)

        # Close the transaction, punch the data, and expect it to pass
        try:
            self.container.close_tx(tx_handle)
            self.log.info("Closed TX for punch dkey test")
            obj.punch_dkeys(0, [dkey])
        except DaosApiError:
            self.fail("dkey punch failed after closing TX")

        # there are a bunch of other cases to test here,
        #    --test punching the same updating and punching the same data in
        #    the same tx, should fail
        #    --test non updated data in an open tx, should work

    def test_akey_punch(self):
        """The most basic test of the akey punch function.

        :avocado: tags=all,daily_regression
        :avocado: tags=vm
        :avocado: tags=object,akey
        :avocado: tags=ObjectPunchTest,test_akey_punch
        """
        # create an object and write some data into it
        dkey = b"this is the dkey"
        data1 = [(b"this is akey 1", b"this is data value 1"),
                 (b"this is akey 2", b"this is data value 2"),
                 (b"this is akey 3", b"this is data value 3")]
        tx_handle = self.container.get_new_tx()
        self.log.info("Created a new TX for punch akey test")
        obj = self.container.write_multi_akeys(
            dkey, data1, obj_cls=1, txn=tx_handle)
        self.log.info("Committing the TX for punch akey test")
        self.container.commit_tx(tx_handle)
        self.log.info("Committed the TX for punch dkey test")

        # read back the 1st epoch's data and check 1 value just to make sure
        # everything is on the up and up
        readbuf = [(data1[0][0], len(data1[0][1]) + 1),
                   (data1[1][0], len(data1[1][1]) + 1),
                   (data1[2][0], len(data1[2][1]) + 1)]
        retrieved_data = self.container.read_multi_akeys(
            dkey, readbuf, obj, txn=tx_handle)
        if retrieved_data[data1[1][0]] != data1[1][1]:
            self.log.info("middle akey: %s", retrieved_data[data1[1][0]])
            self.fail("data retrieval failure")
        self.log.info("data read matches data written")

        # Punch the data and expect it to fail, since it is not committed
        try:
            obj.punch_akeys(tx_handle, dkey, [data1[1][0]])
            self.fail("expected akey punch to fail")
        except DaosApiError as err:
            self.log.info('akey punch failed as expected')
            self.log.info(err)

        # Close the transaction, punch the data, and expect it to pass
        try:
            self.container.close_tx(tx_handle)
            self.log.info("Closed TX for punch akey test")
            obj.punch_akeys(0, dkey, [data1[1][0]])
        except DaosApiError:
            self.fail("dkey punch failed after closing TX")

    def test_obj_punch(self):
        """
        The most basic test of the object punch function.  Really similar
        to above except the whole object is deleted.

        :avocado: tags=all,daily_regression
        :avocado: tags=vm
        :avocado: tags=object
        :avocado: tags=ObjectPunchTest,test_obj_punch
        """
        # create an object and write some data into it
        thedata = b"a string that I want to stuff into an object"
        dkey = b"this is the dkey"
        akey = b"this is the akey"
        tx_handle = self.container.get_new_tx()
        self.log.info("Created a new TX for punch obj test")
        obj = self.container.write_an_obj(
            thedata, len(thedata) + 1, dkey, akey, obj_cls=1, txn=tx_handle)
        self.log.info("Committing the TX for punch obj test")
        self.container.commit_tx(tx_handle)
        self.log.info("Committed the TX for punch obj test")
        # read the data back and make sure its correct
        thedata2 = self.container.read_an_obj(
            len(thedata) + 1, dkey, akey, obj, txn=tx_handle)
        if thedata != thedata2.value:
            self.log.info("wrote data: %s", thedata)
            self.log.info("read data:  %s", thedata2.value)
            self.fail("data read doesn't match data written")
        self.log.info("data read matches data written")

        # Punch the data and expect it to fail, since it is not committed
        try:
            obj.punch(tx_handle)
            self.fail("expected object punch to fail")
        except DaosApiError as err:
            self.log.info('object punch failed as expected')
            self.log.info(err)

        # Close the transaction, punch the data, and expect it to pass
        try:
            self.container.close_tx(tx_handle)
            self.log.info("Closed TX for punch object test")
            obj.punch(0)
        except DaosApiError:
            self.fail("object punch failed after closing TX")
