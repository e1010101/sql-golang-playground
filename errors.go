package main

import "errors"

var ErrInsufficientFunds = errors.New("insufficient funds for transfer")
var ErrAccountNotFound = errors.New("account not found")
var ErrAccountInactive = errors.New("account is inactive")
var ErrSameAccountTransfer = errors.New("cannot transfer funds to the same account")
var ErrInvalidTransferAmount = errors.New("transfer amount must be positive")
