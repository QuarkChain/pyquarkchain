contract PoSWRentalPool {
    mapping (address => uint16) stakerIdMap_;       // Staker should be not a contract
    mapping (uint16 => uint256) principalMap_;      // Starting from 1
    uint16 totalStakers_;
    uint16 maxStakers_;
    uint256 totalPrincipal_;
    uint256 minerRate_;     // in 1/10000
    address payable miner_;  // miner address, should be payble (not be a smart contract)

    constructor (address payable miner, uint256 minerRate, uint16 maxStakers) public {
        miner_ = miner;
        minerRate_ = minerRate;
        maxStakers_ = maxStakers;
    }

    function AddPrincipal() payable external {
        // Update principalMap
        Payout();

        uint16 stakerId = stakerIdMap_[msg.sender];
        if (stakerId == 0) {
            // New staker
            require(totalStakers_ < maxStakers_);
            totalStakers_ += 1;
            stakerId = totalStakers_;
            stakerIdMap_[msg.sender] = stakerId;
        }

        principalMap_[stakerId] += msg.value;
        totalPrincipal_ = totalPrincipal_ + msg.value;
    }

    function GetDividend() public view returns (uint256) {
        return address(this).balance - totalPrincipal_;
    }

    function WithdrawPrincipal(uint amount) public {
        // The order deduct and send should be enforced.
        require(principalMap_[stakerIdMap_[msg.sender]] >= amount);
        principalMap_[stakerIdMap_[msg.sender]] -= amount;
        totalPrincipal_ -= amount;

        address payable addr = address(uint160(msg.sender));
        addr.transfer(amount);

        Payout();

        // TODO: May remove the staker if the staker's principal is zero.
    }

    function Payout() public {
        uint256 dividend = GetDividend();
        uint256 stakerPayout = dividend * (10000 - minerRate_) / 10000;
        uint256 stakerPayoutPrecise = 0;
        for (uint16 i = 1; i <= totalStakers_; i++) {
            uint256 stakePayoutIndividual = principalMap_[i] * stakerPayout / totalPrincipal_;
            stakerPayoutPrecise += stakePayoutIndividual;
            principalMap_[i] += stakePayoutIndividual;
        }

        totalPrincipal_ += stakerPayoutPrecise;

        // Ignore failure if miner is a contract.
        // If failure happens, then the miner's reward will be rewarded to stakers (by calling Payout() repeatedly)
        miner_.send(dividend - stakerPayoutPrecise);

        assert(address(this).balance >= totalPrincipal_);
    }

    function SetMiner(address payable miner) public {
        require(msg.sender == miner_);
        Payout();
        miner_ = miner;
    }
}
