package service

// serviceOptions holds configuration for the Service.
type serviceOptions struct {
	authorizer Authorizer
}

// Option configures the Service.
type Option func(*serviceOptions)

// WithAuthorizer sets the authorizer for the service.
func WithAuthorizer(a Authorizer) Option {
	return func(o *serviceOptions) {
		o.authorizer = a
	}
}
