
import torch.nn as nn

class Logit_in_256_128_64_1(nn.Module) : 
    """
    Input -> 256 -> 128 -> 64 -> 1
    """

    def __init__(self, input_dim : int, dropout : float = 0.3) : 
        super().__init__()

        self.net = nn.Sequential(
            nn.Linear(input_dim, 256),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(256, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(64, 1) # raw logit
        )

    def forward(self, x) : 
        return self.net(x).squeeze(1)

class LogitSharedEncoder_128_64_1(nn.Module) : 
    """
    Input is split in half, each half is fed into encoder (MLP): 
    PlrIn -> 128 -> 64 (hA)
    OppIn -> 128 -> 64 (hB)
    the difference is fed into a head (64 -> 1): 
    logit = head(hA - hB)
    """

    def __init__(self, input_dim : int) :  
        super().__init__()

        self.half = input_dim//2

        # Shared encoder: 
        self.encoder = nn.Sequential(
            nn.Linear(self.half, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU()
        )

        # Comparison head
        self.head = nn.Linear(64, 1)

    def forward(self, x) :
        player = x[:, :self.half]
        opponent = x[:, self.half:]

        hA = self.encoder(player)
        hB = self.encoder(opponent)

        diff = hA - hB
        logit = self.head(diff) 

        return logit.squeeze(-1)

