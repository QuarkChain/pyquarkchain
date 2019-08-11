pragma solidity ^0.5.1;

contract PoSWRental{
    address payable owner_;
    address payable miner_;  // miner address, should be payble (not be a smart contract)
    uint256 principal_;
    uint256 minerRate_;     // in 1/10000

    constructor (address payable owner, uint256 minerRate) public {
        owner_ = owner;
        minerRate_ = minerRate;
        principal_ = address(this).balance;
    }

    function AddPrincipal() payable external {
        principal_ = principal_ + msg.value;
    }

    function WithdrawPrincipal(uint amount) public {
        require(msg.sender == owner_);
        require(principal_ >= amount);
        owner_.transfer(amount);
        principal_ = principal_ - amount;
    }

    function GetDivident() public view returns (uint256) {
        return address(this).balance - principal_;
    }

    function Payout() public {
        uint256 amount = GetDivident() * minerRate_ / 10000;

        // Ignore failure if miner is a contract
        miner_.send(amount);

        principal_ = address(this).balance;
    }

    function SetMiner(address payable miner) public {
        require(msg.sender == owner_);
        Payout();
        miner_ = miner;
    }
}
