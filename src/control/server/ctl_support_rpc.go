//
// (C) Copyright 2019-2022 Intel Corporation.
//
// SPDX-License-Identifier: BSD-2-Clause-Patent
//

package server

import (
	"golang.org/x/net/context"

	ctlpb "github.com/daos-stack/daos/src/control/common/proto/ctl"
	"github.com/daos-stack/daos/src/control/lib/support"
)

// CollectLog collect the file for each server on given target location.
func (c *ControlService) CollectLog(ctx context.Context, req *ctlpb.CollectLogReq) (*ctlpb.CollectLogResp, error) {
	c.log.Infof("Support CollectLog: Calling Log Function: %s, For Cmd/Log: %s", req.LogFunction, req.LogCmd)

	params := support.Params{}
	params.TargetFolder = req.TargetFolder
	params.CustomLogs = req.CustomLogs
	params.JsonOutput = req.JsonOutput
	params.LogFunction = req.LogFunction
	params.LogCmd = req.LogCmd

	err := support.CollectSupportLog(c.log, params)
	if err != nil {
		return nil, err
	}

	resp := new(ctlpb.CollectLogResp)
	return resp, nil
}
