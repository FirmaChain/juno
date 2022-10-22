package types

import (
	"fmt"
	"strings"
)

var CustomAccountParser = []string{ // for firmachain
	"ownerAddress", "creator", "toAddress", "granter", "grantee", "owner", "withdraw_address", "contract", "new_admin", "admin",
}

var DefaultAccountParser = []string{
	"signer", "sender", "to_address", "from_address", "delegator_address",
	"validator_address", "submitter", "proposer", "depositor", "voter",
	"validator_dst_address", "validator_src_address",
}

func ParseAddressInMsg(inputAddresses string, msgText string) (addresses string) {

	orgTotalLength := len(msgText)

	if orgTotalLength > 0 {

		totalLength := orgTotalLength
		msgTempText := msgText

		for i := 0; i < totalLength; i++ {
			idx := strings.Index(msgTempText, "firma1")
			if idx != -1 {
				const lenghOfAddress = 44
				tempAddress := msgTempText[idx : idx+lenghOfAddress]
				msgTempText = msgTempText[idx+lenghOfAddress:]
				totalLength = len(msgTempText)
				i = 0

				if !strings.Contains(addresses, tempAddress) && !strings.Contains(inputAddresses, tempAddress) {
					addresses += tempAddress + ","
				}
			}
		}

		totalLength = orgTotalLength
		msgTempText = msgText

		for i := 0; i < totalLength; i++ {
			idx := strings.Index(msgTempText, "firmavaloper1")
			if idx != -1 {
				const lenghOfValidatorAddress = 51
				tempAddress := msgTempText[idx : idx+lenghOfValidatorAddress]
				msgTempText = msgTempText[idx+lenghOfValidatorAddress:]
				totalLength = len(msgTempText)
				i = 0

				if !strings.Contains(addresses, tempAddress) && !strings.Contains(inputAddresses, tempAddress) {
					addresses += tempAddress + ","
				}
			}
		}
	}

	return addresses
}

func MessageParser(msg map[string]interface{}) (addresses string) {
	accountParser := append(DefaultAccountParser, CustomAccountParser...)

	addresses += "{"
	for _, role := range accountParser {
		if address, ok := msg[role].(string); ok {
			if !strings.Contains(addresses, address) {
				addresses += address + ","
			}
		}
	}

	msgType := msg["@type"].(string)[1:]

	if msgType == "firmachain.firmachain.contract.MsgCreateContractFile" {

		msgText := fmt.Sprint(msg["ownerList"])
		parsedAddresses := ParseAddressInMsg(addresses, msgText)

		if len(parsedAddresses) > 0 {
			addresses += parsedAddresses
		}
	}

	if msgType == "cosmos.authz.v1beta1.MsgExec" {

		msgText := fmt.Sprint(msg["msgs"])
		parsedAddresses := ParseAddressInMsg(addresses, msgText)

		if len(parsedAddresses) > 0 {
			addresses += parsedAddresses
		}
	}

	if input, ok := msg["input"].([]map[string]interface{}); ok {
		for _, i := range input {
			addresses += i["address"].(string) + ","
		}
	}

	if output, ok := msg["output"].([]map[string]interface{}); ok {
		for _, i := range output {
			addresses += i["address"].(string) + ","
		}
	}

	if len(addresses) == 1 {
		return "{}"
	}

	addresses = addresses[:len(addresses)-1] // remove trailing ,
	addresses += "}"

	return addresses
}
