
import torch
import torch.nn as nn

class Logit_in_256_128_64_1(nn.Module) : 
    """
    Input -> 256 -> 128 -> 64 -> 1
    """

    def __init__(self, input_dim : int, dropout : float = 0.3) : 
        super().__init__()

        self.net = nn.Sequential(
            nn.Linear(input_dim, 256),
            nn.ReLU(),

            nn.Linear(256, 128),
            nn.ReLU(),

            nn.Linear(128, 64),
            nn.ReLU(),

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

class LogitEncoder_128_96_Head_32_1(nn.Module) :
    """
    Input is split in half, each half is fed into MLP encoder (e): 
    PlrIn (A) -> 128 -> 96 (eA)
    OppIn (B) -> 128 -> 96 (eB)
    An MLP head (h) takes concatenated input from the encoder, e.g. 
    eAB = cat(eA, eB) (192) -> 96 -> 32 -> 1 
    The logit output is symmetric by = h(eAB) - h(eBA) = -(h(eBA) - h(eAB))
    So in summary: 
    logit(A, B) = h(cat(e(A), e(B))) - h(cat(e(B), e(A)))
    logit(B, A) = -logit(A, B); logit(A, A) = 0
    The intention of this model architecture is that all decks, whether player
    or opponent, will be forced through the same encoder and thus will force the encoder
    to learn some universal structure about decks. 
    The head will then hopefully be able to learn how to contrast encoder outputs to predict who wins.
    """

    def __init__(self, input_dim : int, dropout : float = 0.2) :  
        super().__init__()

        self.half = input_dim//2

        # Shared encoder MLP: 
        self.encoder = nn.Sequential(
            nn.Linear(self.half, 128),
            nn.ReLU(),
            nn.Linear(128, 96),
            nn.ReLU()
        )

        # Head MLP
        self.head = nn.Sequential(
            nn.Linear(192, 96),
            nn.ReLU(),
            nn.Linear(96, 32),
            nn.ReLU(),
            nn.Linear(32, 1)
        )

    def forward(self, x) :
        A = x[:, :self.half] # Player cards as one-hot
        B = x[:, self.half:] # Opponent cards as one-hot

        eA = self.encoder(A)
        eB = self.encoder(B)

        eAB = torch.cat([eA, eB], dim = 1) # encoder outputs concatenated with player first
        eBA = torch.cat([eB, eA], dim = 1) # encoder outputs concatenated with opponent first

        logit = self.head(eAB) - self.head(eBA)

        return logit.squeeze(-1)
    

class LogitSymmetric_256_128_64_1(nn.Module) :
    """
    Version of the above which doesn't involve encoders
    Raw input is fed into MLP (input -> 256 -> 128 -> 64 -> 1)
    logit = u(A, B) - u(B, A)
    """

    def __init__(self, input_dim : int) :  
        super().__init__()

        self.half = input_dim//2

        # MLP
        self.head = nn.Sequential(
            nn.Linear(input_dim, 256),
            nn.ReLU(),
            nn.Linear(256, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 1)
        )

    def forward(self, x) :
        A = x[:, :self.half] # Player cards as one-hot
        B = x[:, self.half:] # Opponent cards as one-hot

        AB = torch.cat([A, B], dim = 1) # encoder outputs concatenated with player first
        BA = torch.cat([B, A], dim = 1) # encoder outputs concatenated with opponent first

        logit = self.head(AB) - self.head(BA)

        return logit.squeeze(-1)
    
    
class LogitSymmetric_128_64_1(nn.Module) :
    """
    'Small'
    (input -> 128 -> 64 -> 1)
    logit = u(A, B) - u(B, A)
    """

    def __init__(self, input_dim : int, dropout : float = 0.2) :  
        super().__init__()

        self.half = input_dim//2

        # MLP
        self.head = nn.Sequential(
            nn.Linear(input_dim, 128),
            #nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(128, 64),
            #nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(64, 1)
        )

    def forward(self, x) :
        A = x[:, :self.half] # Player cards as one-hot
        B = x[:, self.half:] # Opponent cards as one-hot

        AB = torch.cat([A, B], dim = 1) # encoder outputs concatenated with player first
        BA = torch.cat([B, A], dim = 1) # encoder outputs concatenated with opponent first

        logit = self.head(AB) - self.head(BA)

        return logit.squeeze(-1)


class LogitSymmetric_512_256_128_64_1(nn.Module) :
    """
    'Big'
    (input -> 512 -> 256 -> 128 -> 64 -> 1)
    logit = u(A, B) - u(B, A)
    """

    def __init__(self, input_dim : int, dropout : float = 0.2) :  
        super().__init__()

        self.half = input_dim//2

        # MLP
        self.head = nn.Sequential(
            nn.Linear(input_dim, 512),
            #nn.BatchNorm1d(512),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(512, 256),
            #nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(256, 128),
            #nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(128, 64),
            #nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(64, 1)
        )

    def forward(self, x) :
        A = x[:, :self.half] # Player cards as one-hot
        B = x[:, self.half:] # Opponent cards as one-hot

        AB = torch.cat([A, B], dim = 1) # encoder outputs concatenated with player first
        BA = torch.cat([B, A], dim = 1) # encoder outputs concatenated with opponent first

        logit = self.head(AB) - self.head(BA)

        return logit.squeeze(-1)
    

class LogitSymmetric_128_64_1(nn.Module) :
    """
    'Small'
    (input -> 128 -> 64 -> 1)
    logit = u(A, B) - u(B, A)
    """

    def __init__(self, input_dim : int, dropout : float = 0.2) :  
        super().__init__()

        self.half = input_dim//2

        # MLP
        self.head = nn.Sequential(
            nn.Linear(input_dim, 128),
            #nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(128, 64),
            #nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(64, 1)
        )

    def forward(self, x) :
        A = x[:, :self.half] # Player cards as one-hot
        B = x[:, self.half:] # Opponent cards as one-hot

        AB = torch.cat([A, B], dim = 1) # encoder outputs concatenated with player first
        BA = torch.cat([B, A], dim = 1) # encoder outputs concatenated with opponent first

        logit = self.head(AB) - self.head(BA)

        return logit.squeeze(-1)
    

class LogitSymmetric_32_8_1(nn.Module) :
    """
    'Very small'
    (input -> 32 -> 8 -> 1)
    logit = u(A, B) - u(B, A)
    """

    def __init__(self, input_dim : int, dropout : float = 0.2) :  
        super().__init__()

        self.half = input_dim//2

        # MLP
        self.head = nn.Sequential(
            nn.Linear(input_dim, 32),
            #nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(32, 8),
            #nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(8, 1)
        )

    def forward(self, x) :
        A = x[:, :self.half] # Player cards as one-hot
        B = x[:, self.half:] # Opponent cards as one-hot

        AB = torch.cat([A, B], dim = 1) # encoder outputs concatenated with player first
        BA = torch.cat([B, A], dim = 1) # encoder outputs concatenated with opponent first

        logit = self.head(AB) - self.head(BA)

        return logit.squeeze(-1)
    


class LogitSymmetric_8_1(nn.Module) :
    """
    'Super small'
    (input -> 8 -> 1)
    logit = u(A, B) - u(B, A)
    """

    def __init__(self, input_dim : int, dropout : float = 0.2) :  
        super().__init__()

        self.half = input_dim//2

        # MLP
        self.head = nn.Sequential(
            nn.Linear(input_dim, 8),
            #nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(dropout),

            nn.Linear(8, 1),            
        )

    def forward(self, x) :
        A = x[:, :self.half] # Player cards as one-hot
        B = x[:, self.half:] # Opponent cards as one-hot

        AB = torch.cat([A, B], dim = 1) # encoder outputs concatenated with player first
        BA = torch.cat([B, A], dim = 1) # encoder outputs concatenated with opponent first

        logit = self.head(AB) - self.head(BA)

        return logit.squeeze(-1)