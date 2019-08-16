pragma solidity ^0.5.1;

contract RootChainPoSWStaking {
    
    // 3 day locking period.
    uint public constant LOCK_DURATION = 24 * 3600 * 3;
    
    struct Stake {
        bool unlocked;
        uint256 withdrawableTimestamp;
        uint256 amount;
    }
    
    mapping (address => Stake) public stakes;
    
    function () external payable {
        Stake storage stake = stakes[msg.sender];
        require(!stake.unlocked, "should only stake more in locked state");
        
        uint256 newAmount = stake.amount + msg.value;
        require(newAmount > stake.amount, "addition overflow");
        stake.amount = newAmount;
    }
    
    function lock() public {
        Stake storage stake = stakes[msg.sender];
        require(stake.unlocked, "should not lock already-locked accounts");
        
        stake.unlocked = false;
    }
    
    function unlock() public {
        Stake storage stake = stakes[msg.sender];
        require(!stake.unlocked, "should not unlock already-unlocked accounts");
        require(stake.amount > 0, "should have existing stakes");
        
        stake.unlocked = true;
        stake.withdrawableTimestamp = now + LOCK_DURATION;
    }
    
    function withdraw(uint256 amount) public {
        Stake storage stake = stakes[msg.sender];
        require(stake.unlocked && now >= stake.withdrawableTimestamp);
        require(amount <= stake.amount);
        
        stake.amount -= amount;
        
        msg.sender.transfer(amount);
    }
    
    function withdrawAll() public {
        Stake memory stake = stakes[msg.sender];
        require(stake.amount > 0);
        withdraw(stake.amount);
    }    
    
    // Used by root chain for determining stakes.
    function getLockedStakes(address staker) public view returns (uint256) {
        Stake memory stake = stakes[staker];
        if (stake.unlocked) {
            return 0;
        }
        return stake.amount;
    }
  
}
