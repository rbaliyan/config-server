package service

// serviceOptions holds configuration for the Service.
type serviceOptions struct {
	authorizer Authorizer
}

// Option configures the Service.
type Option func(*serviceOptions)

// WithAuthorizer sets the authorizer for the service.
// If a is nil, the option is ignored and the default DenyAll authorizer is kept.
func WithAuthorizer(a Authorizer) Option {
	return func(o *serviceOptions) {
		if a != nil {
			o.authorizer = a
		}
	}
}
