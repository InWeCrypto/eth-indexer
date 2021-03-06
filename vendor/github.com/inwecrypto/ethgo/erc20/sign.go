package erc20

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/inwecrypto/sha3"
)

const (
	signBalanceOf   = "balanceOf(address)"
	signTotalSupply = "totalSupply()"
	signTransfer    = "transfer(address,uint256)"
	eventTransfer   = "Transfer(address,address,uint256)"
	initWallet      = "initWallet(address[],uint256,uint256)"
)

// Method/Event id
var (
	TransferID   = SignABI(signTransfer)
	BalanceOfID  = SignABI(signBalanceOf)
	InitWalletID = SignABI(initWallet)
)

// SignABI sign abi string
func SignABI(abi string) string {
	hasher := sha3.NewKeccak256()
	hasher.Write([]byte(abi))
	data := hasher.Sum(nil)

	return hex.EncodeToString(data[0:4])
}

// BalanceOf create erc20 balanceof abi string
func BalanceOf(address string) string {
	address = strings.Trim(address, "0x")

	return fmt.Sprintf("0x%s%s", SignABI(signBalanceOf), address)
}

func packNumeric(value string, bytes int) string {
	value = strings.TrimSuffix(value, "0x")

	chars := bytes * 2

	n := len(value)
	if n%chars == 0 {
		return value
	}
	return strings.Repeat("0", chars-n%chars) + value
}

// Transfer .
func Transfer(to string, value string) string {
	to = packNumeric(to, 20)
	value = packNumeric(value, 32)

	return fmt.Sprintf("0x%s%s%s", SignABI(signTransfer), to, value)
}
