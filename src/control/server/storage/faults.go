//
// (C) Copyright 2022 Intel Corporation.
//
// SPDX-License-Identifier: BSD-2-Clause-Patent
//

package storage

import (
	"fmt"

	"github.com/daos-stack/daos/src/control/common"
	"github.com/daos-stack/daos/src/control/fault"
	"github.com/daos-stack/daos/src/control/fault/code"
)

const recreateRegionsStr = "Remove regions (and any namespaces) by running the reset subcommand, reboot, then run the prepare subcommand again to recreate regions in AppDirect interleaved mode, reboot and then run the prepare subcommand one more time to create the PMem namespaces"

// FaultScmNotInterleaved creates a fault for the case where the PMem region is in non-interleaved
// mode, this is unsupported.
func FaultScmNotInterleaved(sockID uint) *fault.Fault {
	return storageFault(
		code.ScmBadRegion,
		fmt.Sprintf("PMem region on socket %d is in non-interleaved mode which is unsupported", sockID),
		recreateRegionsStr)
}

// FaultScmNotHealthy creates a fault for the case where the PMem region is in an unhealthy state.
func FaultScmNotHealthy(sockID uint) *fault.Fault {
	return storageFault(
		code.ScmBadRegion,
		fmt.Sprintf("PMem region on socket %d is unhealthy", sockID),
		fmt.Sprintf("Refer to the ipmctl instructions in the troubleshooting section of the DAOS admin guide to check PMem module health and replace faulty PMem modules. %s", recreateRegionsStr))
}

// FaultScmPartialCapacity creates a fault for the case where the PMem region has only partial
// free capacity, this is unsupported.
func FaultScmPartialCapacity(sockID uint) *fault.Fault {
	return storageFault(
		code.ScmBadRegion,
		fmt.Sprintf("PMem region on socket %d only has partial capacity free", sockID),
		"Creating namespaces on regions with partial free-capacity is unsupported, remove namespaces by running the reset subcommand then run the prepare subcommand to recreate namespaces. No reboot is required between commands.")
}

// FaultScmUnknownMemoryMode creates a Fault for the case where the PMem region has an unsupported
// persistent memory type (not AppDirect).
func FaultScmUnknownMemoryMode(sockID uint) *fault.Fault {
	return storageFault(
		code.ScmBadRegion,
		fmt.Sprintf("PMem region on socket %d has an unsupported persistent memory type", sockID),
		recreateRegionsStr)
}

// FaultScmInvalidPMem creates a fault for the case where PMem validation has failed.
func FaultScmInvalidPMem(msg string) *fault.Fault {
	return storageFault(
		code.ScmInvalidPMem,
		"PMem is in an invalid state: "+msg,
		recreateRegionsStr)
}

// FaultScmNoModules represents an error where no PMem modules exist.
var FaultScmNoModules = storageFault(code.ScmNoModules,
	"No PMem modules exist on storage server",
	"Install PMem modules and retry command")

// FaultBdevConfigTypeMismatch represents an error where an incompatible mix of emulated and
// non-emulated NVMe devices are present in the storage config.
var FaultBdevConfigTypeMismatch = storageFault(code.BdevConfigTypeMismatch,
	"A mix of emulated and non-emulated NVMe devices are specified in config",
	"Change config tiers to specify either emulated or non-emulated NVMe devices, but not a mix of both")

// FaultBdevNotFound creates a Fault for the case where no NVMe storage devices
// match expected PCI addresses.
func FaultBdevNotFound(bdevs ...string) *fault.Fault {
	return storageFault(
		code.BdevNotFound,
		fmt.Sprintf("NVMe SSD%s %v not found", common.Pluralise("", len(bdevs)), bdevs),
		fmt.Sprintf("check SSD%s %v that are specified in server config exist",
			common.Pluralise("", len(bdevs)), bdevs),
	)
}

// FaultBdevAccelEngineUnknown creates a Fault when an unrecognized acceleration engine setting is
// detected.
func FaultBdevAccelEngineUnknown(input string, options ...string) *fault.Fault {
	return storageFault(
		code.BdevAccelEngineUnknown,
		fmt.Sprintf("unknown acceleration engine setting %q", input),
		fmt.Sprintf("supported settings are %v, update server config file and restart daos_server",
			options))
}

// FaultBdevAccelOptUnknown creates a Fault when an unrecognized acceleration option is detected.
func FaultBdevAccelOptionUnknown(input string, options ...string) *fault.Fault {
	return storageFault(
		code.BdevAccelOptionUnknown,
		fmt.Sprintf("unknown acceleration option %q", input),
		fmt.Sprintf("supported options are %v, update server config file and restart daos_server",
			options))
}

var FaultBdevNonRootVFIODisable = storageFault(
	code.BdevNonRootVFIODisable,
	"VFIO can not be disabled if running as non-root user",
	"Either run server as root or do not disable VFIO when invoking the command")

var FaultBdevNoIOMMU = storageFault(
	code.BdevNoIOMMU,
	"IOMMU capability is required to access NVMe devices but no IOMMU capability detected",
	"enable IOMMU per the DAOS Admin Guide")

// FaultTargetAlreadyMounted represents an error where the target was already mounted.
var FaultTargetAlreadyMounted = storageFault(
	code.StorageTargetAlreadyMounted,
	"request included already-mounted mount target (cannot double-mount)",
	"unmount the target and retry the operation",
)

// FaultPathAccessDenied represents an error where a mount point or device path for
// a storage target is inaccessible because of a permissions issue.
func FaultPathAccessDenied(path string) *fault.Fault {
	return storageFault(
		code.StoragePathAccessDenied,
		fmt.Sprintf("path %q has incompatible access permissions", path),
		"verify the path is accessible by the user running daos_server and try again",
	)
}

func storageFault(code code.Code, desc, res string) *fault.Fault {
	return &fault.Fault{
		Domain:      "storage",
		Code:        code,
		Description: desc,
		Resolution:  res,
	}
}
