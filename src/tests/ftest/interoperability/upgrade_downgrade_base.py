'''
  (C) Copyright 2023 Intel Corporation.

  SPDX-License-Identifier: BSD-2-Clause-Patent
'''
import os
import random
import time

from general_utils import get_random_bytes
from agent_utils import include_local_host
from command_utils_base import CommandFailure
from ior_test_base import IorTestBase
from run_utils import run_remote


class UpgradeDowngradeBase(IorTestBase):
    # pylint: disable=global-variable-not-assigned,global-statement
    # pylint: disable=too-many-ancestors
    """
    Tests DAOS container attribute get/set/list.
    :avocado: recursive
    """
    def __init__(self, *args, **kwargs):
        """Initialize a ContainerAttributeTest object."""
        super().__init__(*args, **kwargs)
        self.daos_cmd = None
        self.upgrade_repo = ""
        self.downgrade_repo = ""
        self.old_version = ""
        self.new_version = ""

    @staticmethod
    def create_data_set(num_attributes):
        """Create the large attribute dictionary.

        Args:
            num_attributes (int): number of attributes to be created on container.
        Returns:
            dict: a large attribute dictionary
        """
        data_set = {}
        for index in range(num_attributes):
            size = random.randint(1, 10)  # nosec
            key = str(index).encode("utf-8")
            data_set[key] = get_random_bytes(size)
        return data_set

    def verify_list_attr(self, indata, attributes_list):
        """Verify the length of the Attribute names

        Args:
            indata (dict): Dict used to set attr
            attributes_list (list): List obtained from list attr
        """
        length = sum(map(len, indata.keys()))
        size = sum(map(len, attributes_list))

        self.log.info("==Verifying list_attr output:")
        self.log.info("  set_attr names:  %s", list(indata.keys()))
        self.log.info("  set_attr size:   %s", length)
        self.log.info("  list_attr names: %s", attributes_list)
        self.log.info("  list_attr size:  %s", size)

        if length != size:
            self.fail(
                "FAIL: Size does not match for Names in list attr, Expected "
                "len={} and received len={}".format(length, size))
        # verify the Attributes names in list_attr retrieve
        for key in indata.keys():
            if key.decode() not in attributes_list:
                self.fail(
                    "FAIL: Name does not match after list attr, Expected "
                    "buf={} and received buf={}".format(key, attributes_list))

    def verify_pool_attrs(self, pool_attr_dict):
        """"verify pool attributes

        Args:
            pool_attr_dict (dict): expected pool attributes data.
        """
        try:
            pool_attrs = self.daos_cmd.pool_list_attrs(pool=self.pool.identifier, verbose=True)
            self.verify_list_attr(pool_attr_dict, pool_attrs['response'])
        except CommandFailure as error:
            self.fail("Failed to verify pool attributes: {}".format(error))

    def show_daos_version(self, all_hosts, hosts_client):
        """show daos version

        Args:
            all_hosts (NodeSet): all hosts.
            hosts_client (NodeSet): client hosts to show daos and dmg version.
        """
        if not run_remote(self.log, all_hosts, "rpm -qa | grep daos").passed:
            self.fail("Failed to check daos RPMs")
        if not run_remote(self.log, hosts_client, "dmg -i version").passed:  # TODO remove -i
            self.fail("Failed to check dmg vesion")
        if not run_remote(self.log, hosts_client, "daos version").passed:
            self.fail("Failed to check daos version")

    def updowngrade_via_repo(self, servers, clients, repo_1, repo_2):
        """Upgrade downgrade hosts

        Args:
            hosts (NodeSet): test hosts.
            updown (str): upgrade or downgrade
            repo_1 (str): path of the original repository and to be downgraded
            repo_2 (str): path of the new repository to be upgraded
        """
        repo_1_sav = repo_1 + "_sav"
        repo_2_sav = repo_2 + "_sav"
        cmds = [
            "sudo yum remove daos -y",
            "sudo mv {0} {1}".format(repo_1, repo_1_sav),
            "sudo mv {0} {1}".format(repo_2_sav, repo_2),
            "rpm -qa | grep daos",
            "sudo yum install daos-server-tests -y",
            "sudo yum install daos-tests -y",
            "rpm -qa | grep daos"]
        cmds_client = cmds + ["sudo yum install -y ior"]
        cmds_client += ["sudo cp /etc/daos/daos_agent.yml.rpmsave /etc/daos/daos_agent.yml"]
        cmds_client += ["sudo cp /etc/daos/daos_control.yml.rpmsave /etc/daos/daos_control.yml"]
        cmds_svr = cmds + ["sudo cp /etc/daos/daos_server.yml.rpmsave /etc/daos/daos_server.yml"]

        if servers:
            self.log.info("==upgrade_downgrading on servers: %s", servers)
            for cmd in cmds_svr:
                if not run_remote(self.log, servers, cmd).passed:
                    self.fail("Failed to upgrade/downgrade servers via repo")
            self.log.info("==servers upgrade/downgrade success")
            # (5)Restart servers
            self.log.info("==Restart servers after upgrade/downgrade.")
            self.restart_servers()
        if clients:
            self.log.info("==upgrade_downgrading on hosts_client: %s", clients)
            for cmd in cmds_client:
                if not run_remote(self.log, clients, cmd).passed:
                    self.fail("Failed to upgrade/downgrade clients via repo")
            self.log.info("==clients upgrade/downgrade success")

        self.log.info("==sleeping 5 more seconds after upgrade/downgrade")
        time.sleep(5)

    def upgrade(self, servers, clients):
        """Upgrade hosts via repository or RPMs

        Args:
            servers (NodeSet): servers to be upgraded.
            clients (NodeSet): clients to be upgraded.
        """
        if ".repo" in self.upgrade_repo:
            repo_2 = self.upgrade_repo
            repo_1 = self.downgrade_repo
            self.updowngrade_via_repo(servers, clients, repo_1, repo_2)
        else:
            all_hosts = servers + clients
            self.updowngrade_via_rpms(all_hosts, "upgrade", self.upgrade_repo)

    def downgrade(self, servers, clients):
        """Downgrade hosts via repository or RPMs

        Args:
            servers (NodeSet): servers to be upgraded.
            clients (NodeSet): clients to be upgraded.
        """
        if ".repo" in self.upgrade_repo:
            repo_1 = self.upgrade_repo
            repo_2 = self.downgrade_repo
            self.updowngrade_via_repo(servers, clients, repo_1, repo_2)
        else:
            all_hosts = servers + clients
            self.updowngrade_via_rpms(all_hosts, "downgrade", self.downgrade_repo)

    def updowngrade_via_rpms(self, hosts, updown, rpms):
        """Upgrade downgrade hosts

        Args:
            hosts (NodeSet): test hosts.
            updown (str): upgrade or downgrade
            rpms (list): full path of RPMs to be upgrade or downgrade
        """
        cmds = []
        for rpm in rpms:
            cmds.append("sudo yum {} -y {}".format(updown, rpm))
        cmds.append("sudo ipcrm -a")
        cmds.append("sudo ipcs")
        self.log.info("==upgrade_downgrading on hosts: %s", hosts)
        for cmd in cmds:
            if not run_remote(self.log, hosts, cmd).passed:
                self.fail("Failed to upgrade/downgrade via rpms")
        self.log.info("==sleeping 5 more seconds")
        time.sleep(5)
        self.log.info("==upgrade/downgrade via rpms success")

    def daos_ver_after_upgraded(self, host):
        """To display daos and dmg version, and check for error.

        Args:
            host (NodeSet): test host.
        """
        cmds = [
            "daos version",
            "dmg version",
            "daos pool query {}".format(self.pool.identifier)]
        for cmd in cmds:
            if not run_remote(self.log, host, cmd).passed:
                self.fail("Failed to get daos and dmg version after upgrade/downgrade")

    def verify_daos_libdaos(self, step, hosts, cmd, positive_test, agent_server_ver, exp_err=None):
        """Verify daos and libdaos interoperability between different version of agent and server.

        Args:
            step (str): test step for logging.
            hosts (NodeSet): hosts to run command on.
            cmd (str): command to run.
            positive_test (bool): True for positive test, false for negative test.
            agent_server_ver (str): agent and server version.
            exp_err (str, optional): expected error message for negative testcase.
                Defaults to None.
        """
        if positive_test:
            self.log.info("==(%s)Positive_test: %s, on %s", step, cmd, agent_server_ver)
        else:
            self.log.info("==(%s)Negative_test: %s, on %s", step, cmd, agent_server_ver)
        result = run_remote(self.log, hosts, cmd)
        if positive_test:
            if not result.passed:
                self.fail("##({0})Test failed, {1}, on {2}".format(step, cmd, agent_server_ver))
        else:
            if result.passed_hosts:
                self.fail("##({0})Test failed, {1}, on {2}".format(step, cmd, agent_server_ver))
            for stdout in result.all_stdout.values():
                if exp_err not in stdout:
                    self.fail("##({0})Test failed, {1}, on {2}, expect_err {3} "
                              "not shown on stdout".format(step, cmd, agent_server_ver, exp_err))
                
        self.log.info("==(%s)Test passed, %s, on %s", step, cmd, agent_server_ver)

    def has_fault_injection(self, hosts):
        """Check if RPMs with fault-injection function.

        Args:
            hosts (string, list): client hosts to execute the command.

        Returns:
            bool: whether RPMs have fault-injection.
        """
        result = run_remote(self.log, hosts, "daos_debug_set_params -v 67174515")
        if not result.passed:
            self.fail("Failed to check if fault-injection is enabled")
        for stdout in result.all_stdout.values():
            if not stdout.strip():
                return True
        self.log.info("#Host client rpms did not have fault-injection")
        return False

    def enable_fault_injection(self, hosts):
        """Enable fault injection.

        Args:
            hosts (string, list): hosts to enable fualt injection on.
        """
        if not run_remote(self.log, hosts, "daos_debug_set_params -v 67174515").passed:
            self.fail("Failed to enable fault injection")

    def disable_fault_injection(self, hosts):
        """Disable fault injection.

        Args:
            hosts (string, list): hosts to disable fualt injection on.
        """
        if not run_remote(self.log, hosts, "daos_debug_set_params -v 67108864").passed:
            self.fail("Failed to disable fault injection")

    def verify_pool_upgrade_status(self, pool_id, expected_status):
        """Verify pool upgrade status.

        Args:
            pool_id (str): pool to be verified.
            expected_status (str): pool upgrade expected status.
        """
        prop_value = self.get_dmg_command().pool_get_prop(
            pool_id, "upgrade_status")['response'][0]['value']
        if prop_value != expected_status:
            self.fail("##prop_value != expected_status {}".format(expected_status))

    def pool_upgrade_with_fault(self, hosts, pool_id):
        """Execute dmg pool upgrade with fault injection.

        Args:
            hosts (string, list): client hosts to execute the command.
            pool_id (str): pool to be upgraded
        """
        # Verify pool status before upgrade
        self.verify_pool_upgrade_status(pool_id, expected_status="not started")

        # Enable fault-injection
        self.enable_fault_injection(hosts)

        # Pool upgrade
        if not run_remote(self.log, hosts, "dmg pool upgrade {}".format(pool_id)).passed:
            self.fail("dmg pool upgrade failed")
        # Verify pool status during upgrade
        self.verify_pool_upgrade_status(pool_id, expected_status="in progress")
        # Verify pool status during upgrade
        self.verify_pool_upgrade_status(pool_id, expected_status="failed")

        # Disable fault-injection
        self.disable_fault_injection(hosts)
        # Verify pool upgrade resume after removal of fault-injection
        self.verify_pool_upgrade_status(pool_id, expected_status="completed")

    def diff_versions_agent_server(self):
        """Interoperability of different versions of DAOS agent and server.
        Test step:
            (1) Setup
            (2) dmg system stop
            (3) Upgrade 1 server-host to the new version
            (4) Negative test - dmg pool query on mix-version servers
            (5) Upgrade remaining server hosts to the new version
            (6) Restart old agent
            (7) Verify old agent connects to new server, daos and libdaos
            (8) Upgrade agent to the new version
            (9) Verify pool and containers created with new agent and server
            (10) Downgrade server to the old version
            (11) Verify new agent to old server, daos and libdaos
            (12) Downgrade agent to the old version

        """
        # (1)Setup
        self.log.info("==(1)Setup, create pool and container.")
        hosts_client = self.hostlist_clients
        hosts_server = self.hostlist_servers
        all_hosts = include_local_host(hosts_server | hosts_client)
        self.upgrade_repo = self.params.get("upgrade_repo", '/run/interop/*')
        self.downgrade_repo = self.params.get("downgrade_repo", '/run/interop/*')
        self.old_version = self.params.get("old_version", '/run/interop/*')
        self.new_version = self.params.get("new_version", '/run/interop/*')
        self.add_pool(connect=False)
        pool_id = self.pool.identifier
        self.add_container(self.pool)
        self.container.open()
        cmd = "dmg system query"
        positive_test = True
        negative_test = False
        agent_server_ver = f"{self.old_version} agent to {self.old_version} server"
        self.verify_daos_libdaos("1.1", hosts_client, cmd, positive_test, agent_server_ver)

        # (2) dmg system stop
        self.log.info("==(2)Dmg system stop.")
        self.get_dmg_command().system_stop()
        errors = []
        errors.extend(self._stop_managers(self.server_managers, "servers"))
        errors.extend(self._stop_managers(self.agent_managers, "agents"))

        # (3) Upgrade 1 server-host to new
        self.log.info("==(3)Upgrade 1 server to %s.", self.new_version)
        server = hosts_server[0:1]
        self.upgrade(server, [])
        self.log.info("==(3.1)server %s Upgrade to %s completed.", server, self.new_version)

        # (4) Negative test - dmg pool query on mix-version servers
        self.log.info("==(4)Negative test - dmg pool query on mix-version servers.")
        agent_server_ver = f"{self.old_version} agent to mixed-version servers"
        cmd = "dmg pool list"
        exp_err = "unable to contact the DAOS Management Service"
        self.verify_daos_libdaos(
            "4.1", hosts_client, cmd, negative_test, agent_server_ver, exp_err)

        # (5) Upgrade remaining servers to the new version
        server = hosts_server[1:]
        self.log.info("==(5) Upgrade remaining servers %s to %s.", server, self.new_version)
        self.upgrade(server, [])
        self.log.info("==(5.1) server %s Upgrade to %s completed.", server, self.new_version)

        # (6) Restart old agent
        self.log.info("==(6)Restart %s agent", self.old_version)
        self._start_manager_list("agent", self.agent_managers)
        self.show_daos_version(all_hosts, hosts_client)

        # (7)Verify old agent connect to new server
        self.log.info("==(7)Verify %s agent connect to %s server", self.old_version, self.new_version)
        agent_server_ver = f"{self.old_version} agent to {self.new_version} server"
        cmd = "daos pool query {0}".format(pool_id)
        self.verify_daos_libdaos("7.1", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "dmg pool query {0}".format(pool_id)
        exp_err = "admin:0.0.0 are not compatible"
        self.verify_daos_libdaos(
            "7.2", hosts_client, cmd, negative_test, agent_server_ver, exp_err)
        cmd = "sudo daos_agent dump-attachinfo"
        self.verify_daos_libdaos("7.3", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "daos cont create {0} --type POSIX --properties 'rf:2'".format(pool_id)
        self.verify_daos_libdaos("7.4", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "daos pool autotest --pool {0}".format(pool_id)
        self.verify_daos_libdaos("7.5", hosts_client, cmd, positive_test, agent_server_ver)

        # (8) Upgrade agent to the new version
        self.log.info("==(8)Upgrade agent to %s, now %s servers %s agent.",
                      self.new_version, self.new_version, self.new_version)
        self.upgrade([], hosts_client)
        self._start_manager_list("agent", self.agent_managers)
        self.show_daos_version(all_hosts, hosts_client)

        # (9) Pool and containers create on new agent and server
        self.log.info("==(9)Create new pools and containers on %s agent to %s server",
                      self.new_version, self.new_version)
        agent_server_ver = f"{self.new_version} agent to {self.new_version} server"
        cmd = "dmg pool create --size 5G New_pool1"
        self.verify_daos_libdaos("9.1", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "dmg pool list"
        self.verify_daos_libdaos("9.2", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "daos cont create New_pool1 C21 --type POSIX --properties 'rf:2'"
        self.verify_daos_libdaos("9.3", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "daos cont create New_pool1 C22 --type POSIX --properties 'rf:2'"
        self.verify_daos_libdaos("9.4", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "daos container list New_pool1"
        self.verify_daos_libdaos("9.5", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "sudo daos_agent dump-attachinfo"
        self.verify_daos_libdaos("9.6", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "daos pool autotest --pool New_pool1"
        self.verify_daos_libdaos("9.7", hosts_client, cmd, positive_test, agent_server_ver)

        # (10) Downgrade server to the old version
        self.log.info("==(10) Downgrade server to %s, now %s agent to %s server.",
                      self.old_version, self.new_version, self.old_version)
        self.log.info("==(10.1) Dmg system stop.")
        self.get_dmg_command().system_stop()
        errors = []
        errors.extend(self._stop_managers(self.server_managers, "servers"))
        errors.extend(self._stop_managers(self.agent_managers, "agents"))
        self.log.info("==(10.2) Downgrade server to %s", self.old_version)
        self.downgrade(hosts_server, [])
        self.log.info("==(10.3) Restart %s agent", self.old_version)
        self._start_manager_list("agent", self.agent_managers)
        self.show_daos_version(all_hosts, hosts_client)

        # (11) Verify new agent to old server
        agent_server_ver = f"{self.new_version} agent to {self.old_version} server"
        cmd = "daos pool query {0}".format(pool_id)
        self.verify_daos_libdaos("11.1", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "dmg pool query {0}".format(pool_id)
        exp_err = "does not match"
        self.verify_daos_libdaos(
            "11.2", hosts_client, cmd, negative_test, agent_server_ver, exp_err)
        cmd = "sudo daos_agent dump-attachinfo"
        self.verify_daos_libdaos("11.3", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "daos cont create {0} 'C_oldP' --type POSIX --properties 'rf:2'".format(
            pool_id)
        self.verify_daos_libdaos("11.4", hosts_client, cmd, positive_test, agent_server_ver)
        cmd = "daos cont create New_pool1 'C_newP' --type POSIX --properties 'rf:2'"
        exp_err = "DER_NO_SERVICE(-2039)"
        self.verify_daos_libdaos(
            "11.5", hosts_client, cmd, negative_test, agent_server_ver, exp_err)
        exp_err = "common ERR"
        cmd = "daos pool autotest --pool {0}".format(pool_id)
        self.verify_daos_libdaos(
            "11.6", hosts_client, cmd, negative_test, agent_server_ver, exp_err)

        # (12) Downgrade agent to the old version
        self.log.info("==(12)Agent %s  Downgrade started.", hosts_client)
        self.downgrade([], hosts_client)
        self.log.info("==Test passed")

    def upgrade_and_downgrade(self, fault_on_pool_upgrade=False):
        """upgrade and downgrade test base.
        Test step:
            (1)Setup and show rpm, dmg and daos versions on all hosts
            (2)Create pool, container and pool attributes
            (3)Setup and run IOR
                (3.a)DFS
                (3.b)HDF5
                (3.c)POSIX symlink to a file
            (4)Dmg system stop
            (5)Upgrade RPMs to specified new version
            (6)Restart servers
            (7)Restart agent
                verify pool attributes
                verify IOR data integrity after upgraded
                (7.a)DFS
                (7.b)HDF5
                (7.c)POSIX symlink to a file
            (8)Dmg pool get-prop after RPMs upgraded before Pool upgraded
            (9)Dmg pool upgrade and verification after RPMs upgraded
                (9.a)Enable fault injection during pool upgrade
                (9.b)Normal pool upgrade without fault injection
            (10)Create new pool after rpms Upgraded
            (11)Downgrade and cleanup
            (12)Restart servers and agent

        Args:
            fault_on_pool_upgrade (bool): Enable fault-injection during pool upgrade.
        """
        # (1) Setup
        self.log.info("(1)==Setup and show rpm, dmg and daos versions on all hosts.")
        hosts_client = self.hostlist_clients
        hosts_server = self.hostlist_servers
        all_hosts = include_local_host(hosts_server)
        self.upgrade_repo = self.params.get("upgrade_repo", '/run/interop/*')
        self.downgrade_repo = self.params.get("downgrade_repo", '/run/interop/*')
        self.old_version = self.params.get("old_version", '/run/interop/*')
        self.new_version = self.params.get("new_version", '/run/interop/*')
        num_attributes = self.params.get("num_attributes", '/run/attrtests/*')
        mount_dir = self.params.get("mount_dir", '/run/dfuse/*')
        self.show_daos_version(all_hosts, hosts_client)

        # (2) Create pool container and pool attributes
        self.log.info("(2)==Create pool attributes.")
        self.add_pool(connect=False)
        pool_id = self.pool.identifier
        self.add_container(self.pool)
        self.container.open()
        self.daos_cmd = self.get_daos_command()
        pool_attr_dict = self.create_data_set(num_attributes)
        self.pool.pool.set_attr(data=pool_attr_dict)
        self.verify_pool_attrs(pool_attr_dict)
        self.container.close()
        self.pool.disconnect()

        # (3) Setup and run IOR
        self.log.info("(3)==Setup and run IOR.")
        if not run_remote(self.log, hosts_client, f"mkdir -p {mount_dir}").passed:
            self.fail("Failed to create dfuse mount directory")
        ior_api = self.ior_cmd.api.value
        ior_timeout = self.params.get("ior_timeout", self.ior_cmd.namespace)
        ior_write_flags = self.params.get("write_flags", self.ior_cmd.namespace)
        ior_read_flags = self.params.get("read_flags", self.ior_cmd.namespace)
        testfile = os.path.join(mount_dir, "testfile")
        testfile_sav = os.path.join(mount_dir, "testfile_sav")
        testfile_sav2 = os.path.join(mount_dir, "testfile_sav2")
        symlink_testfile = os.path.join(mount_dir, "symlink_testfile")
        # (3.a) ior dfs
        if ior_api in ("DFS", "POSIX"):
            self.log.info("(3.a)==Run non-HDF5 IOR write and read.")
            self.ior_cmd.update_params(flags=ior_write_flags)
            self.run_ior_with_pool(
                timeout=ior_timeout, create_pool=True, create_cont=True, stop_dfuse=False)
            self.ior_cmd.update_params(flags=ior_read_flags)
            self.run_ior_with_pool(
                timeout=ior_timeout, create_pool=False, create_cont=False, stop_dfuse=False)

        # (3.b)ior hdf5
        elif ior_api == "HDF5":
            self.log.info("(3.b)==Run IOR HDF5 write and read.")
            hdf5_plugin_path = self.params.get("plugin_path", '/run/hdf5_vol/')
            self.ior_cmd.update_params(flags=ior_write_flags)
            self.run_ior_with_pool(
                plugin_path=hdf5_plugin_path, mount_dir=mount_dir,
                timeout=ior_timeout, create_pool=True, create_cont=True, stop_dfuse=False)
            self.ior_cmd.update_params(flags=ior_read_flags)
            self.run_ior_with_pool(
                plugin_path=hdf5_plugin_path, mount_dir=mount_dir,
                timeout=ior_timeout, create_pool=False, create_cont=False, stop_dfuse=False)
        else:
            self.fail("##(3)Unsupported IOR api {}".format(ior_api))

        # (3.c)ior posix test file with symlink
        if ior_api == "POSIX":
            self.log.info("(3.c)==Symlink mounted testfile.")
            cmd_list = [
                "cd {}".format(mount_dir),
                "ls -l {}".format(testfile),
                "cp {0} {1}".format(testfile, testfile_sav),
                "cp {0} {1}".format(testfile, testfile_sav2),
                "ln -vs {0} {1}".format(testfile_sav2, symlink_testfile),
                "diff {0} {1}".format(testfile, testfile_sav),
                "ls -l {}".format(symlink_testfile)
            ]
            for cmd in cmd_list:
                if not run_remote(self.log, hosts_client, cmd).passed:
                    self.fail("Failed to setup dfuse test files with {}".format(cmd))
            self.container.close()
            self.pool.disconnect()
            cmd = "fusermount3 -u {}".format(mount_dir)
            if not run_remote(self.log, hosts_client, cmd).passed:
                self.fail("Failed to unmount dfuse")

        # Verify pool attributes before upgrade
        self.log.info("(3.2)==verify pool attributes before upgrade.")
        self.verify_pool_attrs(pool_attr_dict)

        # (4) dmg system stop
        self.log.info("(4)==Dmg system stop.")
        self.get_dmg_command().system_stop()
        errors = []
        errors.extend(self._stop_managers(self.server_managers, "servers"))
        errors.extend(self._stop_managers(self.agent_managers, "agents"))

        # (5)Upgrade
        self.log.info("(5)==Upgrade RPMs to %s", self.new_version)
        self.upgrade(hosts_server, hosts_client)

        self.log.info("==sleeping 30 more seconds")
        time.sleep(30)
        # (6)Restart servers
        self.log.info("(6)==Restart servers.")
        self.restart_servers()

        # (7)Verification after upgrade
        # Restart agent
        self.log.info("(7.1)====Restarting %s agent after upgrade.", self.new_version)
        self._start_manager_list("agent", self.agent_managers)
        self.show_daos_version(all_hosts, hosts_client)

        self.get_dmg_command().pool_list(verbose=True)
        self.get_dmg_command().pool_query(pool=pool_id)
        self.daos_cmd.pool_query(pool=pool_id)

        # Verify pool attributes
        self.log.info("(7.2)====Verifying pool attributes after upgrade.")
        self.verify_pool_attrs(pool_attr_dict)
        self.daos_ver_after_upgraded(hosts_client)

        # Verify IOR data and symlink
        self.log.info("(7.3)====Verifying container data IOR read.")
        if ior_api == "DFS":
            self.log.info("(7.a)==Run IOR DFS read verification.")
            self.run_ior_with_pool(
                timeout=ior_timeout, create_pool=False, create_cont=False, stop_dfuse=False)
        elif ior_api == "HDF5":
            self.log.info("(7.b)==Run IOR HDF5 read verification.")
            self.run_ior_with_pool(
                plugin_path=hdf5_plugin_path, mount_dir=mount_dir,
                timeout=ior_timeout, create_pool=False, create_cont=False, stop_dfuse=False)
        else:
            self.log.info("(7.c)==Run Symlink check after upgraded.")
            cmd = "dfuse --mountpoint {0} --pool {1} --container {2}".format(
                mount_dir, pool_id, self.container.identifier)
            if not run_remote(self.log, hosts_client, cmd).passed:
                self.fail("Failed to mount dfuse")
            cmd = "diff {0} {1}".format(testfile, testfile_sav)
            if not run_remote(self.log, hosts_client, cmd).passed:
                self.fail("dfuse files differ after upgrade")
            cmd = "diff {0} {1}".format(symlink_testfile, testfile_sav2)
            if not run_remote(self.log, hosts_client, cmd).passed:
                self.fail("dfuse files differ after upgrade")

        # (8)Dmg pool get-prop
        self.log.info("(8)==Dmg pool get-prop after RPMs upgraded before Pool upgraded")
        if not run_remote(self.log, hosts_client, "dmg pool get-prop {}".format(pool_id)).passed:
            self.fail("Failed to get pool properties after RPM upgrade")

        # (9)Pool property verification after upgraded
        self.log.info("(9)==Dmg pool upgrade and get-prop after RPM upgrade")

        if fault_on_pool_upgrade and self.has_fault_injection(hosts_client):
            self.log.info("(9.1a)==Pool upgrade with fault-injection.")
            self.pool_upgrade_with_fault(hosts_client, pool_id)
        else:
            self.log.info("(9.1b)==Pool upgrade.")
            cmd = "dmg pool upgrade {}".format(pool_id)
            if not run_remote(self.log, hosts_client, cmd).passed:
                self.fail("Failed to upgrade pool")

        if not run_remote(self.log, hosts_client, "dmg pool get-prop {}".format(pool_id)).passed:
            self.fail("Failed to get pool properties after pool upgrade")

        self.log.info("(9.2)==verify pool attributes after pool-upgraded.")
        self.verify_pool_attrs(pool_attr_dict)
        self.pool.destroy()

        # (10)Create new pool
        self.log.info("(10)==Create new pool after rpms Upgraded")
        self.add_pool(connect=False)
        pool2_id = self.pool.identifier
        self.get_dmg_command().pool_list(verbose=True)
        self.get_dmg_command().pool_query(pool=pool2_id)
        self.daos_cmd.pool_query(pool=pool2_id)
        if not run_remote(self.log, hosts_client, "dmg pool get-prop {}".format(pool_id)).passed:
            self.fail("Failed to get pool properties of new pool after RPM upgrade")

        # (11)Downgrade and cleanup
        self.log.info("(11)==Downgrade and cleanup.")
        if ior_api == "POSIX":
            cmd = "fusermount3 -u {}".format(mount_dir)
            if not run_remote(self.log, hosts_client, cmd).passed:
                self.fail("Failed to unmount dfuse")
        self.container.close()
        self.pool.disconnect()
        self.pool.destroy()
        self.get_dmg_command().system_stop()
        errors = []
        errors.extend(self._stop_managers(self.server_managers, "servers"))
        errors.extend(self._stop_managers(self.agent_managers, "agents"))
        self.log.info("(11.1)==Downgrade RPMs to %s", self.old_version)
        self.downgrade(hosts_server, hosts_client)
        self.log.info("==sleeping 30 more seconds")
        time.sleep(30)

        # (12)Cleanup restart server and agent
        self.log.info("(12)==Restart %s servers and agent.", self.old_version)
        self.restart_servers()
        self._start_manager_list("agent", self.agent_managers)
        self.show_daos_version(all_hosts, hosts_client)
        if fault_on_pool_upgrade and not self.has_fault_injection(hosts_client):
            self.fail("##(12)Upgraded-rpms did not have fault-injection feature.")
        self.log.info("==(12)Test passed")
