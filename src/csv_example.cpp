#include "iex_decoder.h"

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
  // Get the input pcap file as an argument.
  if (argc < 2) {
    std::cout << "Usage: iex_pcap_decoder <input_pcap>" << std::endl;
    return 1;
  }

  // Open a file stream for writing output to csv.
  std::ofstream out_stream;
  try {
    out_stream.open("quotes.csv");
  } catch (...) {
    std::cout << "Exception thrown opening output file." << std::endl;
    return 1;
  }

  // Add the header.
  out_stream << "Timestamp,Symbol,Side,EventFlags,Size,Price" << std::endl;

  // Initialize decoder object with file path.
  std::string input_file(argv[1]);
  IEXDecoder decoder;
  if (!decoder.OpenFileForDecoding(input_file)) {
    std::cout << "Failed to open file '" << input_file << "'." << std::endl;
    return 1;
  }

  // Get the first message from the pcap file.
  std::unique_ptr<IEXMessageBase> msg_ptr;
  auto ret_code = decoder.GetNextMessage(msg_ptr);

  // Main loop to loop through all messages.
  for (; ret_code == ReturnCode::Success; ret_code = decoder.GetNextMessage(msg_ptr)) {

    // For quick message introspection:
    // msg_ptr->Print();
    // Uncommenting this will completely dominate your terminal with output.

    // There are many different message types. Here we just look for quote update (L1 tick).
    if (msg_ptr->GetMessageType() == MessageType::PriceLevelUpdateBuy) {

      // Cast it to the derived type.
      auto quote_msg = dynamic_cast<PriceLevelUpdateMessage*>(msg_ptr.get());
        char side[] = "buy";
      // Check the pointer and write all L1 ticks for ticker 'AMD' to file.
      if (quote_msg) {
        out_stream << quote_msg->timestamp << ","
                   << quote_msg->symbol << ","
                   << side << ","
                   << quote_msg->flags << ","
                   << quote_msg->size << ","
                   << quote_msg->price << std::endl;
             }
    }
              // There are many different message types. Here we just look for quote update (L1 tick).
    if (msg_ptr->GetMessageType() == MessageType::PriceLevelUpdateSell) {

      // Cast it to the derived type.
      auto quote_msg = dynamic_cast<PriceLevelUpdateMessage*>(msg_ptr.get());
        char side[] = "sell";
      // Check the pointer and write all L1 ticks for ticker 'AMD' to file.
      if (quote_msg) {
        out_stream << quote_msg->timestamp << ","
                   << quote_msg->symbol << ","
                   << side << ","
                   << quote_msg->flags << ","
                   << quote_msg->size << ","
                   << quote_msg->price << std::endl;





      }
    }
  }

  out_stream.close();
  return 0;
}
