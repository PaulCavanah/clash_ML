
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