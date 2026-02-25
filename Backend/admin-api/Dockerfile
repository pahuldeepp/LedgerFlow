FROM ruby:3.3

# Set working directory
WORKDIR /app

# Install dependencies
RUN apt-get update -qq && apt-get install -y \
  build-essential \
  libpq-dev \
  nodejs \
  protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*

# Install bundler
RUN gem install bundler

# Copy Gemfiles and install dependencies
COPY Gemfile Gemfile.lock ./
RUN bundle install

# Copy the rest of the application
COPY . .

# Expose port 3000 for Rails server
EXPOSE 3000

# Start Rails server
CMD ["bash", "-c", "bundle exec bootsnap precompile --gemfile app/ lib/ && rm -f tmp/pids/server.pid && bundle exec rails server -b 0.0.0.0 -p 3000"]
