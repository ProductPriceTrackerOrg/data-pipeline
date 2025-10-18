"""
LSTM Sequence-to-Sequence Model for Price Forecasting
"""

import torch
import torch.nn as nn


class LSTMSeq2Seq(nn.Module):
    """LSTM Sequence-to-Sequence Model for Price Forecasting"""

    def __init__(
        self,
        input_size=1,
        hidden_size=128,
        num_layers=2,
        output_size=1,
        forecast_length=7,
    ):
        super(LSTMSeq2Seq, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.forecast_length = forecast_length

        # Encoder LSTM
        self.encoder = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)

        # Decoder LSTM
        self.decoder = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)

        # Output layer
        self.linear = nn.Linear(hidden_size, output_size)

    def forward(self, src):
        """Forward pass through the model"""
        batch_size = src.size(0)

        # Encode
        src = src.unsqueeze(-1)  # Add feature dimension [batch, seq, 1]
        encoder_out, (hidden, cell) = self.encoder(src)

        # Decode - generate forecast_length predictions
        decoder_input = src[:, -1:, :]  # Use last input as first decoder input
        outputs = []

        for _ in range(self.forecast_length):
            decoder_out, (hidden, cell) = self.decoder(decoder_input, (hidden, cell))
            output = self.linear(decoder_out)
            outputs.append(output)
            decoder_input = output  # Use prediction as next input

        return torch.cat(outputs, dim=1).squeeze(-1)  # [batch, forecast_length]


class TimeSeriesDataset(torch.utils.data.Dataset):
    """Dataset class for time series data"""

    def __init__(self, sequences, targets):
        """
        Initialize dataset

        Args:
            sequences: Input sequences [num_samples, sequence_length]
            targets: Target sequences [num_samples, forecast_length]
        """
        self.sequences = torch.FloatTensor(sequences)
        self.targets = torch.FloatTensor(targets)

    def __len__(self):
        return len(self.sequences)

    def __getitem__(self, idx):
        return self.sequences[idx], self.targets[idx]
