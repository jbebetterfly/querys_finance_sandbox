# Finance Department Data Warehouse

Welcome to the Finance Department's Data Warehouse! This repository serves as a comprehensive collection of SQL queries designed to create and manage our databases. Here, you'll find everything you need to efficiently handle our financial data.

## 🛠️ Repository Overview

This repository, lovingly curated by **Juaco**, our query master, is dedicated to enhancing our data management practices. Each query is crafted with precision to ensure optimal performance and accuracy in our financial operations.

## 📂 Directory Structure

- **/CAC**: Contains SQL queries used to create and manipulate our databases. At the moment, we have the cac antiguo and cac nuevo. Remember, a CAC calculation has 2 main components. Expenses and new members!
- **/Expenses**: Contains the only query we use to transform **expenses_base** in to **cubo_financiero**.
- **/Revenues**: At the moment, we've got 2 main querys. Revenue_Cubo is used to classify, analyze and QA'ing each provision/invoice. Buildups query is used to create revenues with the buildups structure with a much less detailed view.
- **/Rewards**: We've got 2 specific Querys. This are both intended to add specific revenue lines to seperate Rewards Revenue tables. This makes sure Revenue_Cubo (updates) grabs this revenues from the revenue_rewards_co_mx table.

## 📖 Getting Started

To get started with the Finance Department's Data Warehouse:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/finance-data-warehouse.git
Navigate to the Queries: Explore the /queries folder to find the SQL scripts you need.

Run the Queries: Follow the documentation in /documentation to execute the queries effectively.

👥 Contributing
We welcome contributions from all team members! If you have queries to add or improvements to suggest, please follow these steps:

Fork the repository.
Create a new branch (git checkout -b feature/YourFeature).
Make your changes and commit them (git commit -m 'Add new query').
Push to the branch (git push origin feature/YourFeature).
Open a pull request.
📞 Contact
For any questions or feedback, feel free to reach out to Juaco directly at [jbezanilla@betterfly.com].

🌟 Acknowledgements
Thanks to the entire Finance team for your hard work and dedication. Together, we can ensure our data is as robust and insightful as possible!