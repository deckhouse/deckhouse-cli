package cmd

import (
	"os"

	kubelogincmd "github.com/int128/kubelogin/pkg/cmd"
	writer2 "github.com/int128/kubelogin/pkg/credentialplugin/writer"
	"github.com/int128/kubelogin/pkg/infrastructure/browser"
	"github.com/int128/kubelogin/pkg/infrastructure/clock"
	"github.com/int128/kubelogin/pkg/infrastructure/logger"
	"github.com/int128/kubelogin/pkg/infrastructure/mutex"
	"github.com/int128/kubelogin/pkg/infrastructure/reader"
	"github.com/int128/kubelogin/pkg/oidc/client"
	"github.com/int128/kubelogin/pkg/tlsclientconfig/loader"
	"github.com/int128/kubelogin/pkg/tokencache/repository"
	"github.com/int128/kubelogin/pkg/usecases/authentication"
	"github.com/int128/kubelogin/pkg/usecases/authentication/authcode"
	"github.com/int128/kubelogin/pkg/usecases/authentication/devicecode"
	"github.com/int128/kubelogin/pkg/usecases/authentication/ropc"
	"github.com/int128/kubelogin/pkg/usecases/credentialplugin"
	"github.com/int128/kubelogin/pkg/usecases/standalone"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

func getKubeloginCmd() *kubelogincmd.Cmd {
	clockReal := &clock.Real{}
	loggerInterface := logger.New()
	browserBrowser := &browser.Browser{}

	factory := &client.Factory{
		Loader: loader.Loader{},
		Clock:  clockReal,
		Logger: loggerInterface,
	}
	authcodeBrowser := &authcode.Browser{
		Browser: browserBrowser,
		Logger:  loggerInterface,
	}
	readerReader := &reader.Reader{
		Stdin: os.Stdin,
	}
	keyboard := &authcode.Keyboard{
		Reader: readerReader,
		Logger: loggerInterface,
	}
	ropcROPC := &ropc.ROPC{
		Reader: readerReader,
		Logger: loggerInterface,
	}
	deviceCode := &devicecode.DeviceCode{
		Browser: browserBrowser,
		Logger:  loggerInterface,
	}
	authenticationAuthentication := &authentication.Authentication{
		ClientFactory:    factory,
		Logger:           loggerInterface,
		Clock:            clockReal,
		AuthCodeBrowser:  authcodeBrowser,
		AuthCodeKeyboard: keyboard,
		ROPC:             ropcROPC,
		DeviceCode:       deviceCode,
	}

	root := &kubelogincmd.Root{
		Standalone: &standalone.Standalone{},
		Logger:     loggerInterface,
	}
	getToken := &credentialplugin.GetToken{
		Authentication:       authenticationAuthentication,
		TokenCacheRepository: &repository.Repository{},
		Writer: &writer2.Writer{
			Stdout: os.Stdout,
		},
		Mutex: &mutex.Mutex{
			Logger: loggerInterface,
		},
		Logger: loggerInterface,
	}
	cmdGetToken := &kubelogincmd.GetToken{
		GetToken: getToken,
		Logger:   loggerInterface,
	}
	cmdCmd := &kubelogincmd.Cmd{
		Root:     root,
		GetToken: cmdGetToken,
		Setup:    &kubelogincmd.Setup{},
		Logger:   loggerInterface,
	}

	return cmdCmd
}

var (
	loginLong = templates.LongDesc(`
		Log in to the Deckhouse Kubernetes Platform.

		UNDER DEVELOPMENT. THE ONLY COMMAND AVAILABLE: get-token

		First-time users of the client should run this command to connect to a server,
		establish an authenticated session, and save connection to the configuration file. The
		default configuration will be saved to your home directory under
		".kube/config".

		The information required to login -- like username and password, a session token, or
		the server details -- can be provided through flags. If not provided, the command will
		prompt for user input as needed. It is also possible to login through a web browser by
		providing the respective flag.`)

	loginExample = templates.Examples(`
		# Log in to the external OIDC issuer
		d8 login get-token --oidc-issuer-url=https://dex.example.com --oidc-client-id=kubeconfig-generator --oidc-client-secret=YOUR_SECRET`)
)

func init() {
	klCmd := getKubeloginCmd()

	loginCmd := &cobra.Command{
		Use:           "login",
		Short:         "Log in to the Deckhouse Kubernetes Platform",
		Long:          loginLong,
		Example:       loginExample,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	klCmd.Logger.AddFlags(loginCmd.PersistentFlags())

	getTokenCmd := klCmd.GetToken.New()
	loginCmd.AddCommand(getTokenCmd)

	rootCmd.AddCommand(loginCmd)
}
